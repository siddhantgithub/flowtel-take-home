import { Pool } from "pg";
import { ApiClient, CursorExpiredError } from "./api";
import { bulkInsertEvents, bulkInsertEventsUnnest, saveCheckpoint, getEventCount } from "./db";
import { IngestionStats, RawEvent } from "./types";
import { Config } from "./config";

const TARGET = 3_000_000;

export class WorkerPool {
  private client: ApiClient;
  private pool: Pool;
  private config: Config;
  private stats: IngestionStats;
  private useCopy: boolean = true;

  constructor(client: ApiClient, pool: Pool, config: Config, initialCount: number = 0) {
    this.client = client;
    this.pool = pool;
    this.config = config;
    this.stats = {
      totalSaved: initialCount,
      startTime: Date.now(),
      lastLogTime: Date.now(),
    };
  }

  get totalSaved(): number {
    return this.stats.totalSaved;
  }

  private async insertEvents(events: RawEvent[]): Promise<number> {
    if (!this.useCopy) {
      return bulkInsertEventsUnnest(this.pool, events);
    }
    try {
      return await bulkInsertEvents(this.pool, events);
    } catch (err: any) {
      // If COPY fails for any reason, switch to unnest for the rest of the run
      console.warn(`COPY failed (${err.message}) — switching to unnest INSERT`);
      this.useCopy = false;
      return bulkInsertEventsUnnest(this.pool, events);
    }
  }

  // Primary: single feed stream (no rate limit, ~5000/page, ~3.5s/page)
  async runFeed(initialCursor?: string | null, sinceMs?: number | null): Promise<number> {
    let cursor: string | undefined = initialCursor ?? undefined;
    let pages = 0;
    let pendingInsert: Promise<number> | null = null;
    let consecutiveEmpty = 0;
    let useSince = sinceMs ?? undefined; // only used on first request (no cursor)

    if (useSince) {
      console.log(`[Feed] Resuming with since=${new Date(useSince).toISOString()}`);
    }

    // Monitor progress in background
    const monitor = setInterval(async () => {
      try {
        this.stats.totalSaved = await getEventCount(this.pool);
        this.logProgress();
      } catch {}
    }, 10_000);

    try {
      while (this.stats.totalSaved < TARGET) {
        let response;
        try {
          ({ response } = await this.client.fetchFeed(
            cursor, this.config.batchSize,
            cursor ? undefined : useSince,  // since only on first request
            undefined
          ));
        } catch (err: any) {
          if (err instanceof CursorExpiredError) {
            console.warn(`[Feed] Cursor expired — resetting. Progress preserved via dedup.`);
            cursor = undefined;
            // Keep useSince so we don't re-fetch from the very beginning
            continue;
          }
          throw err;
        }
        // Clear useSince after first successful fetch — cursor takes over
        useSince = undefined;
        pages++;

        // Flush previous insert
        if (pendingInsert) await pendingInsert;

        if (response.data.length > 0) {
          consecutiveEmpty = 0;
          pendingInsert = this.insertEvents(response.data);
        } else {
          consecutiveEmpty++;
          pendingInsert = null;
          if (consecutiveEmpty >= 10) {
            console.warn("[Feed] 10 consecutive empty pages — stopping.");
            break;
          }
        }

        // Save checkpoint every 10 pages (~35s) — cursor expires in ~120s, so keep it fresh
        if (pages % 10 === 0) {
          if (pendingInsert) { await pendingInsert; pendingInsert = null; }
          this.stats.totalSaved = await getEventCount(this.pool);
          await saveCheckpoint(this.pool, response.pagination.nextCursor ?? null, this.stats.totalSaved);
          console.log(`[Feed] ${pages} pages, checkpoint saved at ${this.stats.totalSaved}`);
        }

        if (!response.pagination.hasMore || !response.pagination.nextCursor) {
          if (pendingInsert) { await pendingInsert; pendingInsert = null; }
          console.log(`[Feed] Done after ${pages} pages.`);
          break;
        }

        cursor = response.pagination.nextCursor;
      }
    } finally {
      // Always flush pending insert before exiting
      if (pendingInsert) {
        try { await pendingInsert; } catch (e: any) {
          console.error(`[Feed] Error flushing pending insert: ${e.message}`);
        }
      }
      clearInterval(monitor);
    }

    this.stats.totalSaved = await getEventCount(this.pool);
    await saveCheckpoint(this.pool, null, this.stats.totalSaved);
    return this.stats.totalSaved;
  }

  // Fallback: /events endpoint (rate-limited 10 req/min)
  async runPipelined(initialCursor?: string | null, sinceMs?: number | null): Promise<number> {
    let cursor = initialCursor ?? undefined;
    let pendingInsert: Promise<number> | null = null;
    let pages = 0;
    let consecutiveEmpty = 0;
    let useSince = sinceMs ?? undefined;

    if (useSince) {
      console.log(`[Events] Resuming with since=${new Date(useSince).toISOString()}`);
    }

    while (this.stats.totalSaved < TARGET) {
      let response;
      const extraParams: Record<string, string> | undefined =
        !cursor && useSince ? { since: String(useSince) } : undefined;
      try {
        ({ response } = await this.client.fetchEvents(cursor, this.config.batchSize, extraParams));
      } catch (err: any) {
        if (err instanceof CursorExpiredError) {
          console.warn(`[Events] Cursor expired — resetting. Progress preserved via dedup.`);
          cursor = undefined;
          continue;
        }
        throw err;
      }
      useSince = undefined;
      pages++;

      if (pendingInsert) await pendingInsert;

      if (response.data.length > 0) {
        consecutiveEmpty = 0;
        pendingInsert = this.insertEvents(response.data);
      } else {
        consecutiveEmpty++;
        pendingInsert = null;
        if (consecutiveEmpty >= 10) {
          console.warn("[Events] 10 consecutive empty pages — stopping.");
          break;
        }
      }

      // Checkpoint every 5 pages
      if (pages % 5 === 0) {
        if (pendingInsert) { await pendingInsert; pendingInsert = null; }
        this.stats.totalSaved = await getEventCount(this.pool);
        await saveCheckpoint(this.pool, cursor ?? null, this.stats.totalSaved);
      }

      this.logProgress();

      if (!response.pagination.hasMore || !response.pagination.nextCursor) {
        if (pendingInsert) { await pendingInsert; pendingInsert = null; }
        this.stats.totalSaved = await getEventCount(this.pool);
        console.log("No more events from API.");
        break;
      }

      cursor = response.pagination.nextCursor;
    }

    // Flush any remaining pending insert
    if (pendingInsert) {
      try { await pendingInsert; } catch (e: any) {
        console.error(`[Events] Error flushing pending insert: ${e.message}`);
      }
    }

    await saveCheckpoint(this.pool, null, this.stats.totalSaved);
    return this.stats.totalSaved;
  }

  logProgress(): void {
    const now = Date.now();
    if (now - this.stats.lastLogTime < 10_000) return;

    const elapsed = (now - this.stats.startTime) / 1000;
    const rate = Math.round(this.stats.totalSaved / elapsed);
    const pct = ((this.stats.totalSaved / TARGET) * 100).toFixed(1);
    const remaining = TARGET - this.stats.totalSaved;
    const eta = rate > 0 ? Math.round(remaining / rate) : 0;
    const etaMin = Math.floor(eta / 60);
    const etaSec = eta % 60;
    const method = this.useCopy ? "COPY" : "unnest";

    console.log(
      `[Progress] ${this.stats.totalSaved.toLocaleString()} / ${TARGET.toLocaleString()} (${pct}%) | ${rate} evt/s | ETA: ${etaMin}m ${etaSec}s | ${method}`
    );

    this.stats.lastLogTime = now;
  }
}
