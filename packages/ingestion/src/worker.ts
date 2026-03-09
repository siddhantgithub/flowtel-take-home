import { Pool } from "pg";
import { ApiClient } from "./api";
import { bulkInsertEvents, bulkInsertEventsUnnest, saveCheckpoint, getEventCount } from "./db";
import { IngestionStats, RawEvent } from "./types";
import { Config } from "./config";

const TARGET = 3_000_000;
const COPY_TIMEOUT_MS = 15_000;

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
      return await Promise.race([
        bulkInsertEvents(this.pool, events),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("COPY timeout")), COPY_TIMEOUT_MS)
        ),
      ]);
    } catch (err: any) {
      if (err.message === "COPY timeout") {
        console.warn("COPY timed out — switching to unnest INSERT");
        this.useCopy = false;
        return bulkInsertEventsUnnest(this.pool, events);
      }
      throw err;
    }
  }

  // Primary: single feed stream (no rate limit, ~5000/page, ~3.5s/page)
  async runFeed(): Promise<number> {
    let cursor: string | undefined;
    let pages = 0;
    let pendingInsert: Promise<number> | null = null;

    // Monitor progress in background
    const monitor = setInterval(async () => {
      try {
        this.stats.totalSaved = await getEventCount(this.pool);
        this.logProgress();
      } catch {}
    }, 10_000);

    try {
      while (this.stats.totalSaved < TARGET) {
        const { response } = await this.client.fetchFeed(cursor, this.config.batchSize);
        pages++;

        if (pendingInsert) await pendingInsert;

        if (response.data.length > 0) {
          pendingInsert = this.insertEvents(response.data);
        } else {
          pendingInsert = null;
        }

        if (pages % 50 === 0) {
          console.log(`[Feed] ${pages} pages fetched`);
        }

        if (!response.pagination.hasMore || !response.pagination.nextCursor) {
          if (pendingInsert) await pendingInsert;
          console.log(`[Feed] Done after ${pages} pages.`);
          break;
        }

        cursor = response.pagination.nextCursor;
      }
    } finally {
      clearInterval(monitor);
    }

    this.stats.totalSaved = await getEventCount(this.pool);
    await saveCheckpoint(this.pool, null, this.stats.totalSaved);
    return this.stats.totalSaved;
  }

  // Fallback: /events endpoint (rate-limited 10 req/min)
  async runPipelined(initialCursor?: string | null): Promise<number> {
    let cursor = initialCursor ?? undefined;
    let pendingInsert: Promise<number> | null = null;
    let pages = 0;

    while (this.stats.totalSaved < TARGET) {
      const { response } = await this.client.fetchEvents(cursor, this.config.batchSize);
      pages++;

      if (pendingInsert) await pendingInsert;

      if (response.data.length > 0) {
        pendingInsert = this.insertEvents(response.data);
      } else {
        pendingInsert = null;
      }

      if (pages % 5 === 0) {
        if (pendingInsert) { await pendingInsert; pendingInsert = null; }
        this.stats.totalSaved = await getEventCount(this.pool);
        await saveCheckpoint(this.pool, cursor ?? null, this.stats.totalSaved);
      }

      this.logProgress();

      if (!response.pagination.hasMore || !response.pagination.nextCursor) {
        if (pendingInsert) await pendingInsert;
        this.stats.totalSaved = await getEventCount(this.pool);
        console.log("No more events from API.");
        break;
      }

      cursor = response.pagination.nextCursor;
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
