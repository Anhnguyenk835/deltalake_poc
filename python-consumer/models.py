"""Data models for CDC events."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class CDCSource:
    """Source metadata from Debezium CDC event."""

    version: str
    connector: str
    name: str
    ts_ms: int
    snapshot: str
    db: str
    schema: str
    table: str
    txId: Optional[int] = None
    lsn: Optional[int] = None
    xmin: Optional[int] = None

    @classmethod
    def from_dict(cls, data: dict) -> "CDCSource":
        return cls(
            version=data.get("version", ""),
            connector=data.get("connector", ""),
            name=data.get("name", ""),
            ts_ms=data.get("ts_ms", 0),
            snapshot=data.get("snapshot", "false"),
            db=data.get("db", ""),
            schema=data.get("schema", ""),
            table=data.get("table", ""),
            txId=data.get("txId"),
            lsn=data.get("lsn"),
            xmin=data.get("xmin"),
        )


@dataclass
class CDCEvent:
    """Debezium CDC event model."""

    operation: str  # 'c' (create), 'u' (update), 'd' (delete), 'r' (read/snapshot)
    before: Optional[dict]
    after: Optional[dict]
    source: CDCSource
    ts_ms: int
    transaction: Optional[dict] = None

    @property
    def table_name(self) -> str:
        return self.source.table

    @property
    def schema_name(self) -> str:
        return self.source.schema

    @property
    def full_table_name(self) -> str:
        return f"{self.source.schema}.{self.source.table}"

    @property
    def event_timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.ts_ms / 1000)

    @property
    def operation_name(self) -> str:
        op_map = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}
        return op_map.get(self.operation, "UNKNOWN")

    @classmethod
    def from_dict(cls, data: dict) -> Optional["CDCEvent"]:
        """Parse CDC event from Debezium JSON payload."""
        payload = data.get("payload")
        if not payload:
            return None

        source_data = payload.get("source", {})
        return cls(
            operation=payload.get("op", ""),
            before=payload.get("before"),
            after=payload.get("after"),
            source=CDCSource.from_dict(source_data),
            ts_ms=payload.get("ts_ms", 0),
            transaction=payload.get("transaction"),
        )


@dataclass
class CDCKey:
    """CDC event key model."""

    payload: dict

    @classmethod
    def from_dict(cls, data: dict) -> "CDCKey":
        return cls(payload=data.get("payload", data))

    @property
    def id(self) -> Any:
        return self.payload.get("id")
