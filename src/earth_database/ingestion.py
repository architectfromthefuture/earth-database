"""Hot-path ingestion for canonical memory records."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from earth_database.constraints import MemoryConstraints
from earth_database.derive_memory import derive_observations_from_event
from earth_database.observability import JsonlEventLogger
from earth_database.provenance import content_sha256, runtime_provenance, utc_now
from earth_database.scheduler import Scheduler
from earth_database.storage import EarthStorage, JobRecord, new_id
from earth_database.trust.classifier import classify_trust
from earth_database.trust.injection_scan import find_prompt_injection_indicators, scan_prompt_injection_risk
from earth_database.trust.schema import (
    ContentRole,
    InjectionRisk,
    SourceType,
    TrustMetadata,
    coerce_source_type,
    highest_injection_risk,
)


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
        content_role: str | ContentRole | None = None,
        provenance_note: str | None = None,
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
        trust = _classify_and_scan(
            content=content,
            source_type=_trust_source_type(source_type),
            content_role=content_role,
            provenance_note=provenance_note,
        )
        trust_dict = trust.as_storage_dict()
        metadata_with_trust = {**(metadata or {}), "trust_metadata": trust_dict}
        with self.storage.transaction() as conn:
            self.storage.insert_ingested_item(
                conn=conn,
                item_id=item_id,
                content=content,
                content_hash=content_hash,
                source_uri=source_uri,
                source_type=source_type,
                metadata=metadata_with_trust,
                tags=normalized_tags,
                event_id=event_id,
                provenance_id=provenance_id,
                parent_hash=parent_hash,
                runtime=runtime_provenance(),
                constraints=self.constraints.as_provenance(),
                now_utc=now,
                trust_metadata=trust_dict,
            )
            event_payload = {
                "content": content,
                "filename": (metadata or {}).get("filename") if isinstance(metadata, dict) else None,
                "source_uri": source_uri,
            }
            for observation in derive_observations_from_event(
                event_type="item_ingested",
                payload=event_payload,
                source_type=trust.source_type.value,
                injection_risk=trust.injection_risk.value,
            ):
                self.storage.insert_observation_memory(
                    conn=conn,
                    observation_id=new_id("obs"),
                    source_event_id=event_id,
                    item_id=item_id,
                    observation=observation,
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
        self.logger.emit(
            stage="trust",
            event="trust_classification_applied",
            payload={
                "source_event_id": event_id,
                "source_type": trust.source_type.value,
                "trust_zone": trust.trust_zone.value,
                "content_role": trust.content_role.value,
                "injection_risk": trust.injection_risk.value,
                "reason": "deterministic source classification applied before storage",
            },
        )
        if trust.injection_risk == InjectionRisk.HIGH:
            self.logger.emit(
                stage="trust",
                event="prompt_injection_risk_detected",
                payload={
                    "source_event_id": event_id,
                    "source_type": trust.source_type.value,
                    "trust_zone": trust.trust_zone.value,
                    "injection_risk": trust.injection_risk.value,
                    "indicators": find_prompt_injection_indicators(content),
                    "reason": "deterministic prompt-injection tripwire matched",
                },
            )
        return result


def _classify_and_scan(
    *,
    content: str,
    source_type: str | SourceType | None,
    content_role: str | ContentRole | None,
    provenance_note: str | None,
) -> TrustMetadata:
    trust = classify_trust(
        source_type=source_type,
        content_role=content_role,
        provenance_note=provenance_note,
    )
    scan_text = _safe_scan_text(content)
    injection_risk = highest_injection_risk(trust.injection_risk, scan_prompt_injection_risk(scan_text))
    return TrustMetadata(
        source_type=trust.source_type,
        trust_zone=trust.trust_zone,
        content_role=trust.content_role,
        injection_risk=injection_risk,
        can_instruct=trust.can_instruct,
        can_call_tools=trust.can_call_tools,
        can_override_policy=trust.can_override_policy,
        provenance_note=trust.provenance_note,
    )


def _safe_scan_text(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    try:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        return str(payload)


def _trust_source_type(source_type: str | SourceType | None) -> SourceType:
    normalized = coerce_source_type(source_type)
    if normalized != SourceType.UNKNOWN:
        return normalized
    return SourceType.INTERNAL_EVENT

