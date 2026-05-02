"""Deterministic chunking that preserves trust metadata."""

from __future__ import annotations

from dataclasses import dataclass

from earth_database.trust.schema import TrustMetadata


DEFAULT_CHUNK_CHARS = 2_000
DEFAULT_CHUNK_OVERLAP = 200


@dataclass(frozen=True)
class MemoryChunk:
    """A model-sized content unit with immutable trust context."""

    content: str
    chunk_index: int
    char_start: int
    char_end: int
    token_count: int
    trust: TrustMetadata


def chunk_text(
    content: str,
    trust: TrustMetadata,
    *,
    max_chars: int = DEFAULT_CHUNK_CHARS,
    overlap_chars: int = DEFAULT_CHUNK_OVERLAP,
) -> tuple[MemoryChunk, ...]:
    """Split text into deterministic chunks while carrying trust labels.

    This intentionally uses simple local logic. The pre-LLM security boundary
    should not depend on a model to decide what text exists or what authority it
    has.
    """

    if max_chars < 1:
        raise ValueError("max_chars must be positive")
    if overlap_chars < 0:
        raise ValueError("overlap_chars cannot be negative")
    if overlap_chars >= max_chars:
        raise ValueError("overlap_chars must be smaller than max_chars")

    if not content:
        return ()

    chunks: list[MemoryChunk] = []
    start = 0
    index = 0
    while start < len(content):
        hard_end = min(start + max_chars, len(content))
        end = _prefer_boundary(content, start, hard_end)
        chunk_content = content[start:end]
        chunks.append(
            MemoryChunk(
                content=chunk_content,
                chunk_index=index,
                char_start=start,
                char_end=end,
                token_count=estimate_token_count(chunk_content),
                trust=trust,
            )
        )
        if end == len(content):
            break
        start = max(0, end - overlap_chars)
        index += 1
    return tuple(chunks)


def estimate_token_count(content: str) -> int:
    """Return a cheap deterministic token estimate for budgeting."""

    if not content:
        return 0
    # Roughly approximate model tokens without invoking a tokenizer dependency.
    by_chars = max(1, (len(content) + 3) // 4)
    by_words = len(content.split())
    return max(by_chars, by_words)


def _prefer_boundary(content: str, start: int, hard_end: int) -> int:
    if hard_end >= len(content):
        return len(content)
    window = content[start:hard_end]
    for marker in ("\n\n", "\n", ". ", " "):
        offset = window.rfind(marker)
        if offset > max(0, len(window) // 2):
            return start + offset + len(marker)
    return hard_end
