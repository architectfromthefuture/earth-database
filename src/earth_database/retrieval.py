"""Exact/provenance-first retrieval."""

from __future__ import annotations

from dataclasses import dataclass

from earth_database.constraints import MemoryConstraints
from earth_database.observability import JsonlEventLogger
from earth_database.routing import MemoryRouter, RoutePlan
from earth_database.storage import EarthStorage, ItemRecord
from earth_database.trust.schema import (
    ContentRole,
    InjectionRisk,
    TrustMetadata,
    TrustZone,
    coerce_content_role,
    coerce_injection_risk,
    coerce_source_type,
)
from earth_database.trust.wrappers import wrap_retrieved_content


@dataclass(frozen=True)
class RetrievalResult:
    route: RoutePlan
    items: tuple[ItemRecord, ...]


@dataclass(frozen=True)
class WrappedRetrievedMemory:
    item: ItemRecord
    trust: TrustMetadata
    wrapped_content: str


class MemoryRetriever:
    def __init__(
        self,
        storage: EarthStorage,
        *,
        constraints: MemoryConstraints | None = None,
        router: MemoryRouter | None = None,
        logger: JsonlEventLogger | None = None,
    ):
        self.storage = storage
        self.constraints = constraints or MemoryConstraints()
        self.router = router or MemoryRouter()
        self.logger = logger or JsonlEventLogger(None)
        self.storage.initialize()

    def retrieve(
        self,
        *,
        query: str | None = None,
        item_id: str | None = None,
        content_hash: str | None = None,
        tags: tuple[str, ...] | list[str] | None = None,
        source_uri: str | None = None,
        source_type: str | None = None,
        limit: int = 10,
    ) -> RetrievalResult:
        bounded_limit = self.constraints.validate_limit(limit)
        normalized_tags = self.constraints.normalize_tags(tags)
        route = self.router.plan(
            query=query,
            item_id=item_id,
            content_hash=content_hash,
            tags=normalized_tags,
            source_uri=source_uri,
            source_type=source_type,
        )

        if route.route == "by_id":
            item = self.storage.get_item(item_id or "")
            items = (item,) if item else ()
        elif route.route == "by_hash":
            items = tuple(self.storage.find_by_hash(content_hash or "", limit=bounded_limit))
        elif route.route == "recent":
            items = tuple(self.storage.list_recent(limit=bounded_limit))
        else:
            items = tuple(
                self.storage.search_items(
                    query=query,
                    tags=normalized_tags,
                    source_uri=source_uri,
                    source_type=source_type,
                    content_hash=content_hash,
                    limit=bounded_limit,
                )
            )

        self.logger.emit(
            stage="retrieval",
            event="items_retrieved",
            payload={
                "route": route.route,
                "reason": route.reason,
                "result_count": len(items),
                "limit": bounded_limit,
            },
        )
        return RetrievalResult(route=route, items=items)

    def retrieve_wrapped(self, **kwargs: object) -> tuple[WrappedRetrievedMemory, ...]:
        result = self.retrieve(**kwargs)
        wrapped: list[WrappedRetrievedMemory] = []
        for item in result.items:
            trust = self._trust_for_item(item)
            provenance = self.storage.get_provenance_for_item(item.id)
            wrapped_content = wrap_retrieved_content(
                item.content,
                trust,
                source_label=item.source_uri,
            )
            wrapped.append(
                WrappedRetrievedMemory(
                    item=item,
                    trust=trust,
                    wrapped_content=wrapped_content,
                )
            )
            self.logger.emit(
                stage="trust",
                event="retrieved_content_wrapped",
                payload={
                    "source_type": trust.source_type.value,
                    "trust_zone": trust.trust_zone.value,
                    "injection_risk": trust.injection_risk.value,
                    "reason": "retrieved memory wrapped with authority metadata",
                    "source_event_id": provenance.event_id if provenance else None,
                    "item_id": item.id,
                },
            )
        return tuple(wrapped)

    def _trust_for_item(self, item: ItemRecord) -> TrustMetadata:
        metadata_trust = item.metadata.get("trust_metadata")
        if isinstance(metadata_trust, dict):
            return TrustMetadata(
                source_type=coerce_source_type(metadata_trust.get("source_type")),
                trust_zone=_coerce_trust_zone(metadata_trust.get("trust_zone")),
                content_role=coerce_content_role(metadata_trust.get("content_role")),
                injection_risk=coerce_injection_risk(metadata_trust.get("injection_risk")),
                can_instruct=bool(metadata_trust.get("can_instruct", False)),
                can_call_tools=bool(metadata_trust.get("can_call_tools", False)),
                can_override_policy=bool(metadata_trust.get("can_override_policy", False)),
                provenance_note=metadata_trust.get("provenance_note"),
            )
        return TrustMetadata(
            source_type=coerce_source_type(item.source_type),
            trust_zone=TrustZone.UNTRUSTED_EXTERNAL,
            content_role=ContentRole.EVIDENCE,
            injection_risk=InjectionRisk.LOW,
        )


def _coerce_trust_zone(value: object) -> TrustZone:
    if isinstance(value, TrustZone):
        return value
    if isinstance(value, str):
        try:
            return TrustZone(value)
        except ValueError:
            return TrustZone.UNKNOWN
    return TrustZone.UNKNOWN

