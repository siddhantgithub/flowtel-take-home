# Architecture

## System Overview

```
┌─────────────────────────────────────────────────────┐
│                   Docker Compose                     │
│                                                      │
│  ┌──────────────────────┐   ┌─────────────────────┐ │
│  │  Ingestion Service   │   │   PostgreSQL 16      │ │
│  │  (Node.js 20)        │──▶│                      │ │
│  │                      │   │  ingested_events     │ │
│  │  ┌────────────────┐  │   │  ingestion_checkpts  │ │
│  │  │  Coordinator   │  │   │  _staging (UNLOGGED) │ │
│  │  │       │        │  │   └─────────────────────┘ │
│  │  │  ┌────▼─────┐  │  │                           │
│  │  │  │ WorkerPool│  │  │                           │
│  │  │  └────┬─────┘  │  │                           │
│  │  │       │        │  │                           │
│  │  │  ┌────▼─────┐  │  │                           │
│  │  │  │ ApiClient │──┼──┼──▶ DataSync API          │
│  │  │  └──────────┘  │  │                           │
│  │  └────────────────┘  │                           │
│  └──────────────────────┘                           │
└─────────────────────────────────────────────────────┘
```

## Components

### Entry Point (`index.ts`)
- Loads config from environment variables
- Creates DB pool, runs migrations
- Initializes `ApiClient` and hands off to `Coordinator`
- Global error handlers for `unhandledRejection` / `uncaughtException` → `process.exit(1)` triggers Docker restart

### Coordinator (`coordinator.ts`)
- Orchestrates the full ingestion lifecycle:
  1. Check existing event count (resumability)
  2. Primary strategy: stream feed endpoint (no rate limit)
  3. Fallback: rate-limited `/api/v1/events`
  4. Post-ingestion: create indexes, export event IDs, print `"ingestion complete"`

### ApiClient (`api.ts`)
- Manages two API strategies:
  - **Stream feed** (`/api/v1/events/d4ta/x7k9/feed`) — discovered hidden endpoint, no rate limit, 5000 events/page
  - **Standard events** (`/api/v1/events`) — 10 req/min rate limit, used as fallback
- **Stream token management**: obtains tokens via `POST /internal/dashboard/stream-access`, auto-refreshes before expiry (300s TTL)
- Exponential backoff retry (up to 20 attempts for rate limits, 5 for errors)
- Rate pacing: spaces requests evenly within rate limit windows

### WorkerPool (`worker.ts`)
- **Pipelined fetch+insert**: overlaps network I/O with DB writes — while inserting batch N, fetches batch N+1
- Progress monitoring via periodic `COUNT(*)` from DB (every 10s)
- Checkpointing for resumability

### Database Layer (`db.ts`)
- **Primary insert: COPY protocol** via `pg-copy-streams` + `stream.pipeline()` for proper backpressure handling
  - COPY into UNLOGGED `_staging` table → `INSERT ... ON CONFLICT DO NOTHING` into main table
- **Fallback insert: unnest arrays** — `INSERT ... SELECT FROM unnest(...)` if COPY times out
- Automatic fallback: COPY has a 15s timeout; if exceeded, switches to unnest for remaining ingestion
- `normalizeTimestamp()`: handles epoch ms, epoch seconds, ISO strings, timezone-less strings

## Data Flow

```
API (5000 events/page, ~3.5s)
  │
  ▼
Fetch page N+1 ◄──── overlap ────► Insert page N
  │                                    │
  │                                    ▼
  │                          TRUNCATE _staging
  │                                    │
  │                          COPY data → _staging
  │                                    │
  │                          INSERT INTO ingested_events
  │                          SELECT FROM _staging
  ▼                          ON CONFLICT (id) DO NOTHING
Next cursor
```

## Database Schema

```sql
-- Main table (indexed post-ingestion)
CREATE TABLE ingested_events (
  id           TEXT PRIMARY KEY,
  session_id   TEXT,
  type         TEXT,
  user_id      TEXT,
  timestamp    TIMESTAMPTZ,
  properties   JSONB,
  raw          JSONB,
  ingested_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Staging table for COPY bulk loads (no constraints, no WAL)
CREATE UNLOGGED TABLE _staging (...same columns...);

-- Single-row checkpoint for resumability
CREATE TABLE ingestion_checkpoints (
  id            INTEGER PRIMARY KEY DEFAULT 1,
  cursor        TEXT,
  events_saved  BIGINT DEFAULT 0,
  updated_at    TIMESTAMPTZ DEFAULT NOW(),
  CONSTRAINT single_row CHECK (id = 1)
);
```

**Index strategy**: Indexes on `timestamp` and `session_id` are created AFTER ingestion completes to avoid write amplification during bulk loading.

## Throughput Optimization

| Technique | Impact |
|-----------|--------|
| Hidden stream feed endpoint (no rate limit) | Removes 10 req/min bottleneck |
| COPY protocol with UNLOGGED staging table | ~5-10x faster than row-by-row INSERT |
| Pipelined fetch+insert (overlap I/O) | Hides DB latency behind network latency |
| Deferred index creation | Avoids index maintenance during bulk load |
| `ON CONFLICT DO NOTHING` dedup | Enables safe resume without duplicate checking |

**Observed rate**: ~1500-1667 events/sec (bottlenecked by API response time of ~3.5s per 5000-event page)

## Resilience

- **Resumable**: On restart, checks `ingested_events` count and resumes from last checkpoint cursor
- **Idempotent**: `ON CONFLICT (id) DO NOTHING` makes re-processing safe
- **Auto-restart**: Docker `restart: on-failure` policy
- **Token refresh**: Stream tokens auto-refresh before 300s expiry
- **Fallback chain**: Stream feed → rate-limited `/events` → retry with backoff

## API Discovery

The challenge hints that "the documented API may not be the fastest way." Through exploration of the dashboard application:

1. Found `/internal/dashboard/stream-access` endpoint in the dashboard's JS bundle
2. Requires browser-like `User-Agent` header to authenticate
3. Returns a stream token granting access to `/api/v1/events/d4ta/x7k9/feed`
4. Feed endpoint has no rate limit and returns 5000 events/page
5. Both endpoints share a rate limit budget — running them in parallel is counterproductive
