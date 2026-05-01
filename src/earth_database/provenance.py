"""Provenance helpers for source lineage and hashes."""

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import platform
import sys
from typing import Any

from earth_database import __version__


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def content_sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def runtime_provenance() -> dict[str, Any]:
    return {
        "package_version": __version__,
        "python_version": sys.version.split()[0],
        "platform": f"{platform.system()} {platform.machine()}",
    }

