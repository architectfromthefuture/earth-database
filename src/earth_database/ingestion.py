"""Hot-path ingestion for canonical memory records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from earth_database.constraints import MemoryConstraints
from earth_database.observability import JsonlEventLogger
from earth_database.provenance import content_sha256, runtime_provenance, utc_now
from earth_database.scheduler import Scheduler
from earth_database.storage import EarthStorage, JobRecord, new_id


DEFAULT_BACKGROUND_JOBS = ("build_summary", "build_embedding")


@dataclass(frozen=True)
class IngestResult:
    item_id: str
    event_id: str
    provenance_id: str
    content_hash: str
    scheduled_jobs: tuple[JobRecord, ...]


class IngestionService:
    def __init__(
        self,
        storage: EarthStorage,
        *,
        constraints: MemoryConstraints | None = None,
        scheduler: Scheduler | None = None,
        logger: JsonlEventLogger | None = None,
    ):
        self.storage = storage
        self.constraints = constraints or MemoryConstraints()
        self.logger = logger or JsonlEventLogger(None)
        self.scheduler = scheduler or Scheduler(storage, self.logger)
        self.storage.initialize()

    def ingest_text(
        self,
        *,
        content: str,
        source_uri: str,
        source_type: str = "text",
        tags: tuple[str, ...] | list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        parent_hash: str | None = None,
        schedule_jobs: tuple[str, ...] = DEFAULT_BACKGROUND_JOBS,
    ) -> IngestResult:
        normalized_tags = self.constraints.normalize_tags(tags)
        self.constraints.validate_ingestion(
            content=content,
            source_type=source_type,
            metadata=metadata,
            scheduled_jobs=schedule_jobs,
        )

        now = utc_now()
        item_id = new_id("item")
        event_id = new_id("evt")
        provenance_id = new_id("prov")
        content_hash = content_sha256(content)
        with self.storage.transaction() as conn:
            self.storage.insert_ingested_item(
                conn=conn,
                item_id=item_id,
                content=content,
                content_hash=content_hash,
                source_uri=source_uri,
                source_type=source_type,
                metadata=metadata or {},
                tags=normalized_tags,
                event_id=event_id,
                provenance_id=provenance_id,
                parent_hash=parent_hash,
                runtime=runtime_provenance(),
                constraints=self.constraints.as_provenance(),
                now_utc=now,
            )
            scheduled = self.scheduler.schedule_for_item(
                conn=conn,
                item_id=item_id,
                content_hash=content_hash,
                jobs=schedule_jobs,
                now_utc=now,
            )

        result = IngestResult(
            item_id=item_id,
            event_id=event_id,
            provenance_id=provenance_id,
            content_hash=content_hash,
            scheduled_jobs=tuple(scheduled),
        )
        self.logger.emit(
            stage="ingestion",
            event="item_ingested",
            payload={
                "item_id": item_id,
                "content_hash": content_hash,
                "source_uri": source_uri,
                "source_type": source_type,
                "scheduled_jobs": [job.job_type for job in scheduled],
            },
        )
        return result

