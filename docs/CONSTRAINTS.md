# Constraints

Constraints are first-class in `earth-database`. They keep low-latency behavior predictable and make future extensions auditable.

## Ingestion Constraints

- Content must be non-empty UTF-8 text.
- Content size defaults to 1 MiB per item.
- Source type must be one of the configured allowed source types.
- Tags are normalized to lowercase, trimmed, unique strings.
- Metadata must be JSON-serializable.
- Ingestion may enqueue slow work, but it may not execute slow work.

## Storage Constraints

- SQLite is the source of truth.
- WAL mode must be enabled on every managed connection.
- Canonical writes should happen in one transaction: item, tags, event, provenance, FTS row, and jobs.
- FTS rows are derived for lookup speed, but they are updated transactionally with canonical content.
- Deleting or compacting content must preserve source event IDs and content hashes.

## Retrieval Constraints

- Exact filters run before text ranking.
- Provenance filters can restrict by source URI, source type, content hash, or tag.
- Result limits must be bounded.
- Retrieval must not mutate storage.
- Vector or semantic results, if added, must be wrapped by provenance constraints.

## Routing Constraints

- Routing is read-only.
- Routing decisions are deterministic for the same query and constraints.
- A route plan can choose exact lookup, tag/source filtering, FTS, recent items, or future semantic lookup.
- Learned routing weights may only be updated by slow scheduled jobs.

## Scheduler Constraints

- Jobs must have idempotency keys.
- Jobs must declare job type, due time, attempts, and payload.
- Claiming a job changes status from `pending` to `running`.
- Completing a job changes status to `completed`.
- Failing a job either retries it with a future due time or marks it `failed`.
- Workers must tolerate repeated claims after process crashes.

## Provenance Constraints

- Every item has a SHA-256 content hash.
- Every provenance row links to the event that introduced the content.
- Derived artifacts must retain parent hashes or source event references.
- Runtime provenance should be descriptive, not secret-bearing.

## Observability Constraints

- Observable events are typed JSON records.
- Event payloads should be small and serializable.
- Logs must avoid secrets and private account identifiers.
- Scheduler events include job ID, job type, status transition, attempt count, and error summaries.

## Low-Latency Constraint

The hot path is not allowed to perform network calls, model inference, vector generation, global compaction, or long-running filesystem scans. These are scheduled background jobs.
