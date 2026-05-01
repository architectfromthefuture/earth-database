"""Read-only retrieval routing."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RoutePlan:
    route: str
    reason: str


class MemoryRouter:
    """Deterministic route planner.

    The router deliberately has no storage dependency. Slow-loop policy learning
    can change this module later, but hot-path routing remains read-only.
    """

    def plan(
        self,
        *,
        query: str | None = None,
        item_id: str | None = None,
        content_hash: str | None = None,
        tags: tuple[str, ...] = (),
        source_uri: str | None = None,
        source_type: str | None = None,
    ) -> RoutePlan:
        if item_id:
            return RoutePlan(route="by_id", reason="item_id was provided")
        if content_hash:
            return RoutePlan(route="by_hash", reason="content_hash was provided")
        if query and query.strip():
            return RoutePlan(route="fts", reason="query text was provided")
        if tags or source_uri or source_type:
            return RoutePlan(route="exact_filter", reason="exact filters were provided")
        return RoutePlan(route="recent", reason="no query or filters were provided")

