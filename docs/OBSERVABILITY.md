# Observability

`earth-database` has two complementary observability surfaces:

1. SQLite `events` rows for queryable, canonical memory history.
2. JSONL trace files for append-only operational inspection.

## JSONL Event Shape

```json
{
  "ts_utc": "2026-05-01T16:59:00.000000Z",
  "stage": "ingestion",
  "event": "item_ingested",
  "payload": {
    "item_id": "01HX...",
    "content_hash": "sha256...",
    "source_uri": "demo://note",
    "scheduled_jobs": ["build_summary"]
  }
}
```

## Stages

- `ingestion`: validation, capture, canonical writes, and job scheduling.
- `storage`: schema setup and durable writes.
- `retrieval`: exact, filtered, and FTS lookup.
- `routing`: read-only route selection.
- `scheduler`: job enqueue, claim, completion, retry, and failure.
- `worker`: slow derived work.

## Provenance Block

Each item stores provenance with:

- `item_id`
- `event_id`
- `source_uri`
- `source_type`
- `content_hash`
- `parent_hash`
- `captured_at_utc`
- `runtime`
- `constraints`

The runtime block may include Python version, platform, and package version.
It must not include secrets.

## Scheduler Trace Format

Scheduler trace payloads should include:

- `job_id`
- `job_type`
- `status`
- `attempts`
- `due_at_utc`
- `idempotency_key`
- `item_id`
- `last_error` when present

## Debug Workflow

1. Check the JSONL trace for high-level stage transitions.
2. Query SQLite `events` for canonical item history.
3. Query `provenance` by item ID or content hash.
4. Inspect `jobs` for pending, running, failed, and completed slow-path work.
5. Re-run only idempotent pending or failed jobs.

## Latency Metrics

The scaffold does not include a metrics daemon. Tests and demos can measure
elapsed milliseconds around:

- ingestion command duration
- exact lookup duration
- FTS query duration
- job claim duration

If this becomes a service, expose p50 and p95 for those operations before
adding distributed tracing.
