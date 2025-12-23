"""Shared configuration and models for CDC consumers."""

from .config import (
    KafkaConfig,
    DatalakeConfig,
    TargetConfig,
    DeltaLakeConfig,
    TOPIC_TABLE_MAPPING,
    CDC_TOPICS,
)
from .models import CDCEvent, CDCKey

__all__ = [
    "KafkaConfig",
    "DatalakeConfig",
    "TargetConfig",
    "DeltaLakeConfig",
    "TOPIC_TABLE_MAPPING",
    "CDC_TOPICS",
    "CDCEvent",
    "CDCKey",
]
