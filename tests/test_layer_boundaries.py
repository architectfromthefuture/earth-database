from __future__ import annotations

import tempfile
from pathlib import Path
import unittest

from earth_database.ingestion import IngestionService
from earth_database.retrieval import MemoryRetriever
from earth_database.routing import MemoryRouter
from earth_database.storage import EarthStorage


class LayerBoundaryTests(unittest.TestCase):
    def test_router_is_read_only_and_deterministic(self) -> None:
        router = MemoryRouter()

        first = router.plan(query="memory", tags=("latency",))
        second = router.plan(query="memory", tags=("latency",))

        self.assertEqual(first, second)
        self.assertEqual(first.route, "fts")

    def test_retrieval_does_not_mutate_scheduler_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = EarthStorage(Path(tmp) / "earth.db")
            ingestion = IngestionService(storage)
            retriever = MemoryRetriever(storage)

            ingestion.ingest_text(
                content="Retrieval can inspect memory without mutating scheduled jobs.",
                source_uri="test://readonly",
                source_type="test",
                tags=("routing",),
            )
            before = storage.list_jobs()

            result = retriever.retrieve(query="inspect memory", limit=5)

            after = storage.list_jobs()
            self.assertEqual(result.route.route, "fts")
            self.assertEqual(
                [(job.id, job.status, job.attempts) for job in after],
                [(job.id, job.status, job.attempts) for job in before],
            )


if __name__ == "__main__":
    unittest.main()

