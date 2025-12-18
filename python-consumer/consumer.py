"""Kafka CDC Consumer for processing Debezium events."""

import json
import signal
from typing import Optional

import structlog
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import (
    CDC_TOPICS,
    DatalakeConfig,
    DeltaLakeConfig,
    KafkaConfig,
    TargetConfig,
)
from database import DatabaseHandler
from delta_handler import DeltaLakeHandler
from models import CDCEvent, CDCKey

# Type alias for either delta handler
DeltaHandler = DeltaLakeHandler  # Will be overridden if using Spark

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()


class CDCConsumer:
    """Kafka consumer for CDC events."""

    def __init__(
        self,
        kafka_config: KafkaConfig,
        datalake_config: DatalakeConfig,
        target_config: TargetConfig,
        delta_config: DeltaLakeConfig,
    ) -> None:
        self.kafka_config = kafka_config
        self.delta_config = delta_config
        self.db_handler = DatabaseHandler(datalake_config, target_config)
        self.delta_handler = self._create_delta_handler(delta_config)
        self._consumer: Optional[KafkaConsumer] = None
        self._running = True
        self._setup_signal_handlers()

    def _create_delta_handler(self, delta_config: DeltaLakeConfig):
        """Create appropriate Delta Lake handler based on configuration."""
        if delta_config.use_spark:
            from spark_delta_handler import SparkDeltaHandler
            logger.info(
                "using_spark_delta_handler",
                spark_master=delta_config.spark_master,
            )
            return SparkDeltaHandler(
                base_path=delta_config.base_path,
                spark_master=delta_config.spark_master,
            )
        else:
            logger.info("using_delta_rs_handler")
            return DeltaLakeHandler(delta_config.base_path)

    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown handlers."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame) -> None:
        """Handle shutdown signals."""
        logger.info("shutdown_signal_received", signal=signum)
        self._running = False

    def _create_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer with configuration."""
        return KafkaConsumer(
            *CDC_TOPICS,
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            group_id=self.kafka_config.group_id,
            auto_offset_reset=self.kafka_config.auto_offset_reset,
            enable_auto_commit=self.kafka_config.enable_auto_commit,
            value_deserializer=lambda x: x,                             # Raw bytes, we'll decode manually
            key_deserializer=lambda x: x,                               # Raw bytes
            consumer_timeout_ms=1000,                                   # 1 second timeout for poll
        )

    def start(self) -> None:
        """Start consuming CDC events."""
        logger.info(
            "starting_consumer",
            topics=CDC_TOPICS,
            bootstrap_servers=self.kafka_config.bootstrap_servers,
        )

        self._consumer = self._create_consumer()

        try:
            while self._running:
                # Poll for messages (returns after consumer_timeout_ms)
                message_batch = self._consumer.poll(timeout_ms=1000)

                if not message_batch:
                    continue

                for topic_partition, messages in message_batch.items():
                    for msg in messages:
                        self._process_message(msg)

        except KafkaError as e:
            logger.error("kafka_exception", error=str(e))
            raise
        except Exception as e:
            logger.error("unexpected_exception", error=str(e), exc_info=True)
            raise
        finally:
            self._shutdown()

    def _process_message(self, msg) -> None:
        """Process a single CDC message."""
        topic = msg.topic
        partition = msg.partition
        offset = msg.offset

        try:
            # Parse key and value
            key_data = json.loads(msg.key.decode("utf-8")) if msg.key else {}
            value_data = json.loads(msg.value.decode("utf-8")) if msg.value else {}

            key = CDCKey.from_dict(key_data)

            # Handle tombstone events (null value = delete)
            if not value_data:
                logger.info(
                    "tombstone_event",
                    topic=topic,
                    key=key.payload,
                    partition=partition,
                    offset=offset,
                )
                # Commit offset for tombstone
                self._consumer.commit()
                return

            event = CDCEvent.from_dict(value_data)
            if not event:
                logger.warning(
                    "invalid_event",
                    topic=topic,
                    partition=partition,
                    offset=offset,
                )
                return

            logger.info(
                "processing_event",
                topic=topic,
                table=event.full_table_name,
                operation=event.operation_name,
                record_id=key.id,
                partition=partition,
                offset=offset,
            )

            # Store raw event in data lake
            self.db_handler.store_raw_event(event, key, topic, partition, offset)

            # Apply to processed tables in data lake
            self.db_handler.apply_to_datalake_processed(event)

            # Apply to target database
            self.db_handler.apply_to_target(event)

            # Write to Delta Lake
            if self.delta_config.enable_cdc_events:
                self.delta_handler.write_cdc_event(event, key, topic, partition, offset)

            if self.delta_config.enable_table_snapshots:
                self.delta_handler.write_table_snapshot(event.table_name, event)

            # Commit offset after successful processing
            self._consumer.commit()

            logger.info(
                "event_processed",
                topic=topic,
                table=event.full_table_name,
                operation=event.operation_name,
                record_id=key.id,
            )

        except json.JSONDecodeError as e:
            logger.error(
                "json_decode_error",
                topic=topic,
                partition=partition,
                offset=offset,
                error=str(e),
            )
        except Exception as e:
            logger.error(
                "processing_error",
                topic=topic,
                partition=partition,
                offset=offset,
                error=str(e),
                exc_info=True,
            )

    def _shutdown(self) -> None:
        """Shutdown consumer gracefully."""
        logger.info("shutting_down_consumer")
        if self._consumer:
            self._consumer.close()
        self.db_handler.close()
        # Close Spark session if using SparkDeltaHandler
        if hasattr(self.delta_handler, "close"):
            self.delta_handler.close()
        logger.info("consumer_shutdown_complete")


def main() -> None:
    """Main entry point."""
    logger.info("initializing_cdc_consumer")

    kafka_config = KafkaConfig.from_env()
    datalake_config = DatalakeConfig.from_env()
    target_config = TargetConfig.from_env()
    delta_config = DeltaLakeConfig.from_env()

    logger.info(
        "delta_lake_config",
        base_path=delta_config.base_path,
        enable_cdc_events=delta_config.enable_cdc_events,
        enable_table_snapshots=delta_config.enable_table_snapshots,
        use_spark=delta_config.use_spark,
        spark_master=delta_config.spark_master,
    )

    consumer = CDCConsumer(kafka_config, datalake_config, target_config, delta_config)
    consumer.start()


if __name__ == "__main__":
    main()
