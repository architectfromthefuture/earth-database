from __future__ import annotations

import json
import tempfile
from pathlib import Path
import unittest

from earth_database.ingestion import IngestionService
from earth_database.observability import JsonlEventLogger
from earth_database.retrieval import MemoryRetriever
from earth_database.scheduler import Scheduler
from earth_database.storage import EarthStorage


class MemoryFlowTests(unittest.TestCase):
    def test_ingestion_writes_provenance_and_queues_slow_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            storage = EarthStorage(tmp_path / "earth.db")
            logger = JsonlEventLogger(tmp_path / "events.jsonl")
            scheduler = Scheduler(storage, logger)
            ingestion = IngestionService(storage, scheduler=scheduler, logger=logger)

            result = ingestion.ingest_text(
                content="Latency sensitive memory should keep embeddings out of ingestion.",
                source_uri="test://latency",
                source_type="test",
                tags=("Memory", "Latency", "memory"),
                metadata={"case": "hot-path"},
            )

            item = storage.get_item(result.item_id)
            provenance = storage.get_provenance_for_item(result.item_id)
            jobs = storage.list_jobs(status="pending")

            self.assertIsNotNone(item)
            self.assertEqual(item.tags, ("latency", "memory"))
            self.assertIsNotNone(provenance)
            self.assertEqual(provenance.content_hash, result.content_hash)
            self.assertEqual(provenance.event_id, result.event_id)
            self.assertEqual({job.job_type for job in jobs}, {"build_summary", "build_embedding"})
            self.assertTrue(all(job.status == "pending" for job in jobs))

            log_lines = (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(json.loads(log_lines[-1])["event"], "item_ingested")

    def test_retrieval_uses_exact_filters_and_fts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = EarthStorage(Path(tmp) / "earth.db")
            ingestion = IngestionService(storage)
            retriever = MemoryRetriever(storage)

            first = ingestion.ingest_text(
                content="SQLite FTS keeps exact local memory lookup fast.",
                source_uri="test://sqlite",
                source_type="test",
                tags=("database", "latency"),
            )
            ingestion.ingest_text(
                content="Background workers can build semantic indexes later.",
                source_uri="test://worker",
                source_type="test",
                tags=("worker",),
            )

            by_hash = retriever.retrieve(content_hash=first.content_hash, limit=5)
            by_fts = retriever.retrieve(query="SQLite lookup", tags=("database",), limit=5)

            self.assertEqual(by_hash.route.route, "by_hash")
            self.assertEqual([item.id for item in by_hash.items], [first.item_id])
            self.assertEqual(by_fts.route.route, "fts")
            self.assertEqual([item.id for item in by_fts.items], [first.item_id])

    def test_scheduler_claim_complete_and_idempotency(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = EarthStorage(Path(tmp) / "earth.db")
            scheduler = Scheduler(storage)
            ingestion = IngestionService(storage, scheduler=scheduler)

            ingestion.ingest_text(
                content="Same content should not duplicate idempotent derived jobs.",
                source_uri="test://one",
                source_type="test",
            )
            ingestion.ingest_text(
                content="Same content should not duplicate idempotent derived jobs.",
                source_uri="test://two",
                source_type="test",
            )

            pending = storage.list_jobs(status="pending")
            self.assertEqual(len(pending), 2)

            claimed = scheduler.claim_due_jobs(limit=1)
            self.assertEqual(len(claimed), 1)
            self.assertEqual(claimed[0].status, "running")
            completed = scheduler.complete_job(claimed[0].id)
            self.assertEqual(completed.status, "completed")


if __name__ == "__main__":
    unittest.main()

