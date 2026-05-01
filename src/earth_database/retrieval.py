"""Exact/provenance-first retrieval."""

from __future__ import annotations

from dataclasses import dataclass

from earth_database.constraints import MemoryConstraints
from earth_database.observability import JsonlEventLogger
from earth_database.routing import MemoryRouter, RoutePlan
from earth_database.storage import EarthStorage, ItemRecord


@dataclass(frozen=True)
class RetrievalResult:
    route: RoutePlan
    items: tuple[ItemRecord, ...]


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

