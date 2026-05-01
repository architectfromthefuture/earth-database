"""Append-only JSONL observability for earth-database."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import json
from typing import Any

from earth_database.provenance import utc_now


@dataclass(frozen=True)
class ObservableEvent:
    ts_utc: str
    stage: str
    event: str
    payload: dict[str, Any]


class JsonlEventLogger:
    def __init__(self, log_path: str | Path | None):
        self.log_path = Path(log_path) if log_path else None

    def emit(self, *, stage: str, event: str, payload: dict[str, Any]) -> ObservableEvent:
        record = ObservableEvent(
            ts_utc=utc_now(),
            stage=stage,
            event=event,
            payload=payload,
        )
        if self.log_path is not None:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(asdict(record), sort_keys=True) + "\n")
        return record

