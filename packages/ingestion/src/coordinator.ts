import { Pool } from "pg";
import { ApiClient } from "./api";
import { Config } from "./config";
import { loadCheckpoint, getEventCount, getMaxTimestampMs, exportEventIds, createIndexes } from "./db";
import { WorkerPool } from "./worker";

const TARGET_EVENTS = 3_000_000;
const EVENT_IDS_PATH = "/app/output/event_ids.txt";
const FEED_PAGE_SIZE = 5000;
const MAX_FEED_RETRIES = 10;
const FEED_RETRY_DELAY_MS = 5_000;

export async function runIngestion(client: ApiClient, pool: Pool, config: Config): Promise<void> {
  const existingCount = await getEventCount(pool);

  if (existingCount >= TARGET_EVENTS) {
    console.log(`Already have ${existingCount} events. Skipping ingestion.`);
    await exportEventIds(pool, EVENT_IDS_PATH);
    console.log("ingestion complete");
    return;
  }

  // Get max timestamp for resume — skip already-ingested time range
  const maxTs = await getMaxTimestampMs(pool);
  if (maxTs && maxTs > 0) {
    console.log(`Resume: max timestamp in DB = ${new Date(maxTs).toISOString()} (${maxTs})`);
  }

  console.log(`Existing events: ${existingCount}. Need ${TARGET_EVENTS - existingCount} more.`);
  config.batchSize = FEED_PAGE_SIZE;

  // Try to load saved cursor — if restart was fast enough (<120s), cursor may still be valid
  const checkpoint = await loadCheckpoint(pool);
  let savedCursor: string | null = null;
  if (checkpoint?.cursor) {
    console.log(`Found saved cursor from checkpoint (${checkpoint.eventsSaved} events). Will attempt to resume.`);
    savedCursor = checkpoint.cursor;
  }

  // Primary strategy: stream feed with persistent retry — stay on feed as long as possible
  console.log("Strategy: STREAM FEED (no rate limit, 5000/page)");
  for (let attempt = 1; attempt <= MAX_FEED_RETRIES; attempt++) {
    const currentCount = await getEventCount(pool);
    if (currentCount >= TARGET_EVENTS) break;

    try {
      await client.getStreamAccess();
      const worker = new WorkerPool(client, pool, config, currentCount);
      // First attempt: try saved cursor (may skip re-traversal if restart was fast)
      // Subsequent attempts or if no cursor: start from beginning, dedup handles duplicates
      const cursorToUse = attempt === 1 ? savedCursor : null;
      await worker.runFeed(cursorToUse);
      break; // feed completed successfully
    } catch (error: any) {
      console.error(`[Feed] Attempt ${attempt}/${MAX_FEED_RETRIES} failed: ${error?.message ?? error}`);
      savedCursor = null; // don't reuse a failed cursor
      if (attempt < MAX_FEED_RETRIES) {
        const delay = FEED_RETRY_DELAY_MS * attempt;
        console.log(`[Feed] Retrying in ${delay / 1000}s with fresh token...`);
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }

  // Last resort fallback — only if feed couldn't finish after all retries
  const countAfterFeed = await getEventCount(pool);
  if (countAfterFeed < TARGET_EVENTS) {
    console.log(`[Fallback] Feed got ${countAfterFeed}/${TARGET_EVENTS}. Switching to /events...`);
    const currentMaxTs = await getMaxTimestampMs(pool);
    const fallbackWorker = new WorkerPool(client, pool, config, countAfterFeed);
    await fallbackWorker.runPipelined(null, currentMaxTs);
  }

  // Post-ingestion
  await createIndexes(pool);

  const finalCount = await getEventCount(pool);
  console.log(`Ingestion finished. Total events in DB: ${finalCount}`);

  await exportEventIds(pool, EVENT_IDS_PATH);
  console.log("ingestion complete");
}
