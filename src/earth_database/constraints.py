"""Explicit constraints for hot-path memory operations."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from typing import Any


class ConstraintViolation(ValueError):
    """Raised when a caller requests work outside the configured constraints."""


@dataclass(frozen=True)
class MemoryConstraints:
    max_content_bytes: int = 1_048_576
    max_tags: int = 32
    max_tag_length: int = 64
    max_result_limit: int = 100
    allowed_source_types: tuple[str, ...] = (
        "text",
        "markdown",
        "note",
        "cli",
        "test",
        "user_input",
        "system_generated",
        "internal_event",
        "uploaded_file",
        "external_repo_file",
        "external_webpage",
        "external_email",
        "unknown",
    )
    allowed_job_types: tuple[str, ...] = (
        "build_summary",
        "build_embedding",
        "compact_item",
        "refresh_derived_index",
    )

    def as_provenance(self) -> dict[str, Any]:
        return asdict(self)

    def normalize_tags(self, tags: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
        normalized: list[str] = []
        for tag in tags or ():
            candidate = tag.strip().lower()
            if not candidate:
                continue
            if len(candidate) > self.max_tag_length:
                raise ConstraintViolation(f"tag exceeds {self.max_tag_length} characters: {tag!r}")
            normalized.append(candidate)
        unique = tuple(dict.fromkeys(normalized))
        if len(unique) > self.max_tags:
            raise ConstraintViolation(f"too many tags: {len(unique)} > {self.max_tags}")
        return unique

    def validate_ingestion(
        self,
        *,
        content: str,
        source_type: str,
        metadata: dict[str, Any] | None,
        scheduled_jobs: tuple[str, ...],
    ) -> None:
        if not content or not content.strip():
            raise ConstraintViolation("content must be non-empty")
        content_size = len(content.encode("utf-8"))
        if content_size > self.max_content_bytes:
            raise ConstraintViolation(
                f"content exceeds {self.max_content_bytes} bytes: {content_size}"
            )
        if source_type not in self.allowed_source_types:
            raise ConstraintViolation(f"source_type is not allowed: {source_type}")
        self._validate_json(metadata or {}, "metadata")
        for job_type in scheduled_jobs:
            if job_type not in self.allowed_job_types:
                raise ConstraintViolation(f"job_type is not allowed: {job_type}")

    def validate_limit(self, limit: int) -> int:
        if limit < 1:
            raise ConstraintViolation("limit must be positive")
        if limit > self.max_result_limit:
            raise ConstraintViolation(f"limit exceeds {self.max_result_limit}: {limit}")
        return limit

    def _validate_json(self, value: dict[str, Any], label: str) -> None:
        try:
            json.dumps(value)
        except TypeError as exc:
            raise ConstraintViolation(f"{label} must be JSON-serializable") from exc

