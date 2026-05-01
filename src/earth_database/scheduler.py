"""Explicit scheduler for slow memory work."""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Any

from earth_database.observability import JsonlEventLogger
from earth_database.provenance import utc_now
from earth_database.storage import EarthStorage, JobRecord, new_id, to_json


@dataclass(frozen=True)
class ScheduledJob:
    job_type: str
    payload: dict[str, Any]
    due_at_utc: str | None = None
    idempotency_key: str | None = None


class Scheduler:
    def __init__(self, storage: EarthStorage, logger: JsonlEventLogger | None = None):
        self.storage = storage
        self.logger = logger or JsonlEventLogger(None)

    def schedule_for_item(
        self,
        *,
        conn: sqlite3.Connection,
        item_id: str,
        content_hash: str,
        jobs: tuple[str, ...],
        now_utc: str,
    ) -> list[JobRecord]:
        scheduled: list[JobRecord] = []
        for job_type in jobs:
            idempotency_key = f"{job_type}:{content_hash}"
            job = self.storage.enqueue_job(
                conn=conn,
                job_id=new_id("job"),
                job_type=job_type,
                idempotency_key=idempotency_key,
                item_id=item_id,
                due_at_utc=now_utc,
                payload={"item_id": item_id, "content_hash": content_hash},
                now_utc=now_utc,
            )
            scheduled.append(job)
            self.storage.insert_event(
                conn=conn,
                event_id=new_id("evt"),
                item_id=item_id,
                stage="scheduler",
                event_type="job_scheduled",
                payload={
                    "job_id": job.id,
                    "job_type": job.job_type,
                    "idempotency_key": job.idempotency_key,
                },
                now_utc=now_utc,
            )
        return scheduled

    def claim_due_jobs(self, *, limit: int = 10, now_utc: str | None = None) -> list[JobRecord]:
        now = now_utc or utc_now()
        claimed: list[JobRecord] = []
        with self.storage.transaction() as conn:
            rows = conn.execute(
                """
                SELECT * FROM jobs
                WHERE status = 'pending' AND due_at_utc <= ? AND attempts < max_attempts
                ORDER BY due_at_utc, created_at_utc
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
            for row in rows:
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'running',
                        attempts = attempts + 1,
                        locked_at_utc = ?,
                        updated_at_utc = ?
                    WHERE id = ? AND status = 'pending'
                    """,
                    (now, now, row["id"]),
                )
                updated = conn.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
                job = self.storage._job_from_row(updated)
                claimed.append(job)
                self.storage.insert_event(
                    conn=conn,
                    event_id=new_id("evt"),
                    item_id=job.item_id,
                    stage="scheduler",
                    event_type="job_claimed",
                    payload=self._job_payload(job),
                    now_utc=now,
                )
        for job in claimed:
            self.logger.emit(stage="scheduler", event="job_claimed", payload=self._job_payload(job))
        return claimed

    def complete_job(self, job_id: str, *, now_utc: str | None = None) -> JobRecord:
        now = now_utc or utc_now()
        with self.storage.transaction() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = 'completed',
                    locked_at_utc = NULL,
                    last_error = NULL,
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (now, job_id),
            )
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            job = self.storage._job_from_row(row)
            self.storage.insert_event(
                conn=conn,
                event_id=new_id("evt"),
                item_id=job.item_id,
                stage="scheduler",
                event_type="job_completed",
                payload=self._job_payload(job),
                now_utc=now,
            )
        self.logger.emit(stage="scheduler", event="job_completed", payload=self._job_payload(job))
        return job

    def fail_job(
        self,
        job_id: str,
        *,
        error: str,
        retry_at_utc: str | None = None,
        now_utc: str | None = None,
    ) -> JobRecord:
        now = now_utc or utc_now()
        with self.storage.transaction() as conn:
            current = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if current is None:
                raise KeyError(f"job not found: {job_id}")
            next_status = "pending" if current["attempts"] < current["max_attempts"] else "failed"
            due_at = retry_at_utc or now
            conn.execute(
                """
                UPDATE jobs
                SET status = ?,
                    due_at_utc = ?,
                    locked_at_utc = NULL,
                    last_error = ?,
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (next_status, due_at, error, now, job_id),
            )
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            job = self.storage._job_from_row(row)
            self.storage.insert_event(
                conn=conn,
                event_id=new_id("evt"),
                item_id=job.item_id,
                stage="scheduler",
                event_type="job_failed" if job.status == "failed" else "job_retried",
                payload={**self._job_payload(job), "last_error": error},
                now_utc=now,
            )
        self.logger.emit(
            stage="scheduler",
            event="job_failed" if job.status == "failed" else "job_retried",
            payload={**self._job_payload(job), "last_error": error},
        )
        return job

    def _job_payload(self, job: JobRecord) -> dict[str, Any]:
        return {
            "job_id": job.id,
            "job_type": job.job_type,
            "status": job.status,
            "attempts": job.attempts,
            "due_at_utc": job.due_at_utc,
            "idempotency_key": job.idempotency_key,
            "item_id": job.item_id,
        }

