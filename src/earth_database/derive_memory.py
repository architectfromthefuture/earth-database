"""Derived observation memory helpers."""

from __future__ import annotations

from typing import Any

from earth_database.trust.schema import InjectionRisk


def derive_observations_from_event(
    *,
    event_type: str,
    payload: dict[str, Any],
    source_type: str | None,
    injection_risk: str | None,
) -> tuple[str, ...]:
    observations: list[str] = []
    if event_type in {"item_ingested", "file_ingested"}:
        source_uri = str(payload.get("source_uri") or "unknown source")
        observations.append(f"Content was ingested from {source_uri}.")

    if injection_risk == InjectionRisk.HIGH.value:
        filename = payload.get("filename") or _filename_from_source_uri(payload.get("source_uri"))
        if filename:
            observations.append(
                f"External file {filename} was ingested and classified as "
                "high prompt-injection risk."
            )
        else:
            observations.append(
                f"Content from {source_type or 'unknown'} was ingested and classified as "
                "high prompt-injection risk."
            )
    return tuple(observations)


def _filename_from_source_uri(source_uri: Any) -> str | None:
    if not isinstance(source_uri, str) or not source_uri.strip():
        return None
    normalized = source_uri.rstrip("/")
    if not normalized:
        return None
    candidate = normalized.rsplit("/", 1)[-1]
    return candidate or None
