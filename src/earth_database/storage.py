"""SQLite storage core for earth-database."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import json
import sqlite3
import uuid
from collections.abc import Iterator, Sequence
from typing import Any


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def to_json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True, separators=(",", ":"))


def from_json(value: str | None) -> Any:
    if not value:
        return {}
    return json.loads(value)


@dataclass(frozen=True)
class ItemRecord:
    id: str
    content: str
    content_hash: str
    source_uri: str
    source_type: str
    metadata: dict[str, Any]
    tags: tuple[str, ...]
    created_at_utc: str
    updated_at_utc: str


@dataclass(frozen=True)
class JobRecord:
    id: str
    job_type: str
    idempotency_key: str
    item_id: str | None
    status: str
    due_at_utc: str
    attempts: int
    max_attempts: int
    payload: dict[str, Any]
    locked_at_utc: str | None
    last_error: str | None
    created_at_utc: str
    updated_at_utc: str


@dataclass(frozen=True)
class ProvenanceRecord:
    id: str
    item_id: str
    event_id: str
    source_uri: str
    source_type: str
    content_hash: str
    parent_hash: str | None
    runtime_json: dict[str, Any]
    constraints_json: dict[str, Any]
    captured_at_utc: str


class EarthStorage:
    """Small repository layer around SQLite.

    Each method opens short-lived connections. SQLite WAL mode keeps local readers
    cheap while preserving one durable source of truth for canonical memory.
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.transaction() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id TEXT PRIMARY KEY,
                    content TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    source_uri TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_items_content_hash
                    ON items(content_hash);

                CREATE INDEX IF NOT EXISTS idx_items_source
                    ON items(source_type, source_uri);

                CREATE TABLE IF NOT EXISTS item_tags (
                    item_id TEXT NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    tag TEXT NOT NULL,
                    PRIMARY KEY (item_id, tag)
                );

                CREATE INDEX IF NOT EXISTS idx_item_tags_tag
                    ON item_tags(tag);

                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    item_id TEXT REFERENCES items(id) ON DELETE SET NULL,
                    stage TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    ts_utc TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_events_item
                    ON events(item_id, ts_utc);

                CREATE TABLE IF NOT EXISTS provenance (
                    id TEXT PRIMARY KEY,
                    item_id TEXT NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    event_id TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
                    source_uri TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    parent_hash TEXT,
                    runtime_json TEXT NOT NULL DEFAULT '{}',
                    constraints_json TEXT NOT NULL DEFAULT '{}',
                    captured_at_utc TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_provenance_hash
                    ON provenance(content_hash);

                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    item_id TEXT REFERENCES items(id) ON DELETE CASCADE,
                    status TEXT NOT NULL,
                    due_at_utc TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    locked_at_utc TEXT,
                    last_error TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_due
                    ON jobs(status, due_at_utc);

                CREATE VIRTUAL TABLE IF NOT EXISTS items_fts
                    USING fts5(item_id UNINDEXED, content, source_uri UNINDEXED, tags);
                """
            )

    def insert_ingested_item(
        self,
        *,
        item_id: str,
        content: str,
        content_hash: str,
        source_uri: str,
        source_type: str,
        metadata: dict[str, Any],
        tags: Sequence[str],
        event_id: str,
        provenance_id: str,
        parent_hash: str | None,
        runtime: dict[str, Any],
        constraints: dict[str, Any],
        now_utc: str,
        conn: sqlite3.Connection,
    ) -> None:
        conn.execute(
            """
            INSERT INTO items (
                id, content, content_hash, source_uri, source_type,
                metadata_json, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                content,
                content_hash,
                source_uri,
                source_type,
                to_json(metadata),
                now_utc,
                now_utc,
            ),
        )
        conn.executemany(
            "INSERT INTO item_tags (item_id, tag) VALUES (?, ?)",
            [(item_id, tag) for tag in tags],
        )
        self.insert_event(
            conn=conn,
            event_id=event_id,
            item_id=item_id,
            stage="ingestion",
            event_type="item_ingested",
            payload={
                "content_hash": content_hash,
                "source_uri": source_uri,
                "source_type": source_type,
                "tags": list(tags),
            },
            now_utc=now_utc,
        )
        conn.execute(
            """
            INSERT INTO provenance (
                id, item_id, event_id, source_uri, source_type, content_hash,
                parent_hash, runtime_json, constraints_json, captured_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                provenance_id,
                item_id,
                event_id,
                source_uri,
                source_type,
                content_hash,
                parent_hash,
                to_json(runtime),
                to_json(constraints),
                now_utc,
            ),
        )
        conn.execute(
            "INSERT INTO items_fts (item_id, content, source_uri, tags) VALUES (?, ?, ?, ?)",
            (item_id, content, source_uri, " ".join(tags)),
        )

    def insert_event(
        self,
        *,
        conn: sqlite3.Connection,
        event_id: str,
        item_id: str | None,
        stage: str,
        event_type: str,
        payload: dict[str, Any],
        now_utc: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO events (id, item_id, stage, event_type, payload_json, ts_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_id, item_id, stage, event_type, to_json(payload), now_utc),
        )

    def enqueue_job(
        self,
        *,
        conn: sqlite3.Connection,
        job_id: str,
        job_type: str,
        idempotency_key: str,
        item_id: str | None,
        due_at_utc: str,
        payload: dict[str, Any],
        now_utc: str,
        max_attempts: int = 3,
    ) -> JobRecord:
        conn.execute(
            """
            INSERT OR IGNORE INTO jobs (
                id, job_type, idempotency_key, item_id, status, due_at_utc,
                attempts, max_attempts, payload_json, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, 'pending', ?, 0, ?, ?, ?, ?)
            """,
            (
                job_id,
                job_type,
                idempotency_key,
                item_id,
                due_at_utc,
                max_attempts,
                to_json(payload),
                now_utc,
                now_utc,
            ),
        )
        row = conn.execute(
            "SELECT * FROM jobs WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        return self._job_from_row(row)

    def get_item(self, item_id: str) -> ItemRecord | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM items WHERE id = ?", (item_id,)).fetchone()
            if row is None:
                return None
            tags = self._tags_for_item(conn, item_id)
            return self._item_from_row(row, tags)

    def get_provenance_for_item(self, item_id: str) -> ProvenanceRecord | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM provenance WHERE item_id = ? ORDER BY captured_at_utc LIMIT 1",
                (item_id,),
            ).fetchone()
            if row is None:
                return None
            return self._provenance_from_row(row)

    def find_by_hash(self, content_hash: str, *, limit: int) -> list[ItemRecord]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM items
                WHERE content_hash = ?
                ORDER BY created_at_utc DESC
                LIMIT ?
                """,
                (content_hash, limit),
            ).fetchall()
            return [self._item_from_row(row, self._tags_for_item(conn, row["id"])) for row in rows]

    def list_recent(self, *, limit: int) -> list[ItemRecord]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM items ORDER BY created_at_utc DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [self._item_from_row(row, self._tags_for_item(conn, row["id"])) for row in rows]

    def search_items(
        self,
        *,
        query: str | None,
        tags: Sequence[str] = (),
        source_uri: str | None = None,
        source_type: str | None = None,
        content_hash: str | None = None,
        limit: int = 10,
    ) -> list[ItemRecord]:
        if query:
            return self._search_fts(
                query=query,
                tags=tags,
                source_uri=source_uri,
                source_type=source_type,
                content_hash=content_hash,
                limit=limit,
            )
        return self._search_exact(
            tags=tags,
            source_uri=source_uri,
            source_type=source_type,
            content_hash=content_hash,
            limit=limit,
        )

    def _search_exact(
        self,
        *,
        tags: Sequence[str],
        source_uri: str | None,
        source_type: str | None,
        content_hash: str | None,
        limit: int,
    ) -> list[ItemRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        joins: list[str] = []
        for index, tag in enumerate(tags):
            alias = f"t{index}"
            joins.append(f"JOIN item_tags {alias} ON {alias}.item_id = i.id AND {alias}.tag = ?")
            params.append(tag)
        if source_uri:
            clauses.append("i.source_uri = ?")
            params.append(source_uri)
        if source_type:
            clauses.append("i.source_type = ?")
            params.append(source_type)
        if content_hash:
            clauses.append("i.content_hash = ?")
            params.append(content_hash)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT i.* FROM items i
            {' '.join(joins)}
            {where}
            ORDER BY i.created_at_utc DESC
            LIMIT ?
        """
        params.append(limit)
        with self.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._item_from_row(row, self._tags_for_item(conn, row["id"])) for row in rows]

    def _search_fts(
        self,
        *,
        query: str,
        tags: Sequence[str],
        source_uri: str | None,
        source_type: str | None,
        content_hash: str | None,
        limit: int,
    ) -> list[ItemRecord]:
        clauses = ["items_fts MATCH ?"]
        where_params: list[Any] = [query]
        join_params: list[Any] = []
        joins: list[str] = []
        for index, tag in enumerate(tags):
            alias = f"t{index}"
            joins.append(f"JOIN item_tags {alias} ON {alias}.item_id = i.id AND {alias}.tag = ?")
            join_params.append(tag)
        if source_uri:
            clauses.append("i.source_uri = ?")
            where_params.append(source_uri)
        if source_type:
            clauses.append("i.source_type = ?")
            where_params.append(source_type)
        if content_hash:
            clauses.append("i.content_hash = ?")
            where_params.append(content_hash)
        sql = f"""
            SELECT i.* FROM items_fts
            JOIN items i ON i.id = items_fts.item_id
            {' '.join(joins)}
            WHERE {' AND '.join(clauses)}
            ORDER BY bm25(items_fts), i.created_at_utc DESC
            LIMIT ?
        """
        params = [*join_params, *where_params]
        params.append(limit)
        with self.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._item_from_row(row, self._tags_for_item(conn, row["id"])) for row in rows]

    def list_jobs(self, *, status: str | None = None, limit: int = 100) -> list[JobRecord]:
        if status:
            sql = "SELECT * FROM jobs WHERE status = ? ORDER BY due_at_utc, created_at_utc LIMIT ?"
            params: tuple[Any, ...] = (status, limit)
        else:
            sql = "SELECT * FROM jobs ORDER BY due_at_utc, created_at_utc LIMIT ?"
            params = (limit,)
        with self.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._job_from_row(row) for row in rows]

    def _tags_for_item(self, conn: sqlite3.Connection, item_id: str) -> tuple[str, ...]:
        rows = conn.execute(
            "SELECT tag FROM item_tags WHERE item_id = ? ORDER BY tag",
            (item_id,),
        ).fetchall()
        return tuple(row["tag"] for row in rows)

    def _item_from_row(self, row: sqlite3.Row, tags: Sequence[str]) -> ItemRecord:
        return ItemRecord(
            id=row["id"],
            content=row["content"],
            content_hash=row["content_hash"],
            source_uri=row["source_uri"],
            source_type=row["source_type"],
            metadata=from_json(row["metadata_json"]),
            tags=tuple(tags),
            created_at_utc=row["created_at_utc"],
            updated_at_utc=row["updated_at_utc"],
        )

    def _job_from_row(self, row: sqlite3.Row) -> JobRecord:
        return JobRecord(
            id=row["id"],
            job_type=row["job_type"],
            idempotency_key=row["idempotency_key"],
            item_id=row["item_id"],
            status=row["status"],
            due_at_utc=row["due_at_utc"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            payload=from_json(row["payload_json"]),
            locked_at_utc=row["locked_at_utc"],
            last_error=row["last_error"],
            created_at_utc=row["created_at_utc"],
            updated_at_utc=row["updated_at_utc"],
        )

    def _provenance_from_row(self, row: sqlite3.Row) -> ProvenanceRecord:
        return ProvenanceRecord(
            id=row["id"],
            item_id=row["item_id"],
            event_id=row["event_id"],
            source_uri=row["source_uri"],
            source_type=row["source_type"],
            content_hash=row["content_hash"],
            parent_hash=row["parent_hash"],
            runtime_json=from_json(row["runtime_json"]),
            constraints_json=from_json(row["constraints_json"]),
            captured_at_utc=row["captured_at_utc"],
        )
