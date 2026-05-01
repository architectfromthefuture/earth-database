"""Command line entry points for earth-database."""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from earth_database.ingestion import IngestionService
from earth_database.observability import JsonlEventLogger
from earth_database.retrieval import MemoryRetriever
from earth_database.scheduler import Scheduler
from earth_database.storage import EarthStorage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="earth-database",
        description="Local embedded memory system with observable layers.",
    )
    subcommands = parser.add_subparsers(dest="command")
    subcommands.add_parser("demo", help="run a tiny ingest/retrieve/schedule demo")
    return parser


def run_demo() -> None:
    with tempfile.TemporaryDirectory(prefix="earth-database-") as tmp:
        base = Path(tmp)
        storage = EarthStorage(base / "earth.db")
        logger = JsonlEventLogger(base / "events.jsonl")
        scheduler = Scheduler(storage, logger)
        ingestion = IngestionService(storage, scheduler=scheduler, logger=logger)
        retriever = MemoryRetriever(storage, logger=logger)

        result = ingestion.ingest_text(
            content="Earth database keeps exact memory hot and semantic work scheduled.",
            source_uri="demo://note",
            source_type="note",
            tags=("architecture", "memory", "latency"),
            metadata={"demo": True},
        )
        retrieved = retriever.retrieve(query="semantic scheduled", tags=("memory",), limit=5)
        jobs = storage.list_jobs(status="pending")

        print(f"database: {base / 'earth.db'}")
        print(f"trace: {base / 'events.jsonl'}")
        print(f"item_id: {result.item_id}")
        print(f"content_hash: {result.content_hash}")
        print(f"retrieval_route: {retrieved.route.route}")
        print(f"retrieved_items: {[item.id for item in retrieved.items]}")
        print(f"pending_jobs: {[(job.job_type, job.status) for job in jobs]}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "demo":
        run_demo()
        return 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

