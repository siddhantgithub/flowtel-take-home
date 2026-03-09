# DataSync Analytics - Event Ingestion Service

A production-ready TypeScript ingestion service that extracts all 3,000,000 events from the DataSync Analytics API and stores them in PostgreSQL.

## How to Run

```bash
# Set your API key
export API_KEY=your_api_key_here

# Run the ingestion
sh run-ingestion.sh
```

This starts PostgreSQL 16 and the ingestion service via Docker Compose. The script monitors progress and exits when ingestion is complete. Event IDs are exported to `output/event_ids.txt`.

## Architecture Overview

```
Coordinator
    |
    v
WorkerPool (feed retry loop, up to 10 attempts)
    |
    +---> ApiClient (stream feed / /events fallback)
    |
    +---> DB Layer (COPY protocol / unnest fallback)
```

**Components:**

| Component | File | Responsibility |
|-----------|------|---------------|
| Entry Point | `src/index.ts` | Config, DB pool, global error handlers |
| Coordinator | `src/coordinator.ts` | Orchestration, feed retry loop, fallback strategy |
| WorkerPool | `src/worker.ts` | Pipelined fetch+insert, progress tracking, checkpointing |
| ApiClient | `src/api.ts` | Stream token management, retry with backoff, cursor expiry handling |
| DB Layer | `src/db.ts` | COPY/unnest bulk insert, migrations, deferred indexes |

See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation with diagrams.

## API Discoveries

1. **Hidden stream feed endpoint** - Exploring the dashboard's JavaScript bundle revealed `/internal/dashboard/stream-access`, which returns a token granting access to `/api/v1/events/d4ta/x7k9/feed`. This endpoint has **no rate limit** and returns up to 5,000 events per page.

2. **Stream token auth** - The stream access endpoint requires a browser-like `User-Agent` header and the API key passed as a cookie (`dashboard_api_key`). Tokens expire in 300s.

3. **Cursor structure** - Cursors are base64-encoded JSON containing `{id, ts, v, exp}`. The server does not validate the `exp` field, allowing us to extend cursor expiry to 24 hours on checkpoint save for crash-resilient resume.

4. **Shared rate limit budget** - Both the feed and `/events` endpoints share the same rate limit pool. Running them in parallel is counterproductive.

5. **Timestamp formats** - The API returns timestamps as epoch milliseconds, epoch seconds, and ISO strings inconsistently. The `normalizeTimestamp()` function handles all variants.

## Throughput Optimization

| Technique | Impact |
|-----------|--------|
| Hidden stream feed (no rate limit) | Removes 10 req/min bottleneck |
| COPY protocol with UNLOGGED staging table | ~5-10x faster than row-by-row INSERT |
| Pipelined fetch+insert (overlap I/O) | Hides DB latency behind network latency |
| Deferred index creation | Avoids index maintenance during bulk load |
| `ON CONFLICT DO NOTHING` dedup | Safe resume without duplicate checking |

**Observed throughput:** ~1,000-1,500 events/sec (bottlenecked by API response time of ~3.5s per 5,000-event page).

## Resilience & Resume

- **Cursor resume**: Checkpoints saved every 10 pages with cursor expiry extended to 24h. On restart, resumes from exact position.
- **Feed retry**: Up to 10 attempts with fresh stream token and increasing backoff before falling to `/events`.
- **COPY fallback**: If COPY protocol fails, automatically switches to `unnest`-based array inserts.
- **Cursor expiration**: `CursorExpiredError` resets cursor and continues; dedup handles overlap.
- **Fallback endpoint**: `/events` with `since=maxTimestamp` for true skip-ahead resume as last resort.
- **Docker restart**: `restart: on-failure` policy for automatic recovery.
- **Idempotent writes**: `INSERT ... ON CONFLICT (id) DO NOTHING` makes re-processing safe.

## Testing

```bash
cd packages/ingestion && npm test
```

18 unit tests across 3 suites:
- **normalizeTimestamp** (8 tests) - epoch ms/seconds, ISO strings, edge cases
- **cursorExpiration** (4 tests) - error class, detection from /events and feed, no retry
- **worker** (6 tests) - cursor reset, empty page guards, COPY fallback, pending flush

## What I Would Improve With More Time

- Integration tests against a mock API server
- Prometheus metrics endpoint for monitoring throughput and error rates
- Parallel feed streams if the API supported independent cursors per partition
- Streaming export of event IDs (currently loads all into memory for ORDER BY)
- Circuit breaker pattern for API failures instead of simple retry counter

## AI Tools Used

**Claude Code** (Anthropic's CLI for Claude) was used as a coding partner throughout development. It helped with:
- Exploring the API and dashboard to discover the hidden stream feed endpoint
- Implementing the COPY protocol with proper `stream.pipeline()` backpressure handling
- Debugging cursor expiration and token refresh edge cases
- Writing unit tests for critical resilience paths
- Discovering the cursor expiry extension technique (base64 decode + re-encode with longer TTL)
- Architecture documentation
