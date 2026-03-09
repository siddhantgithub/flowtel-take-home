import { Pool } from "pg";
import { ApiClient } from "./api";
import { Config } from "./config";
import { loadCheckpoint, getEventCount, exportEventIds, createIndexes } from "./db";
import { WorkerPool } from "./worker";

const TARGET_EVENTS = 3_000_000;
const EVENT_IDS_PATH = "/app/output/event_ids.txt";
const FEED_PAGE_SIZE = 5000;

export async function runIngestion(client: ApiClient, pool: Pool, config: Config): Promise<void> {
  const existingCount = await getEventCount(pool);

  if (existingCount >= TARGET_EVENTS) {
    console.log(`Already have ${existingCount} events. Skipping ingestion.`);
    await exportEventIds(pool, EVENT_IDS_PATH);
    console.log("ingestion complete");
    return;
  }

  console.log(`Existing events: ${existingCount}. Need ${TARGET_EVENTS - existingCount} more.`);
  config.batchSize = FEED_PAGE_SIZE;

  const workerPool = new WorkerPool(client, pool, config, existingCount);

  // Primary strategy: stream feed endpoint (no rate limit)
  try {
    console.log("Strategy: STREAM FEED (no rate limit, 5000/page)");
    await client.getStreamAccess();
    await workerPool.runFeed();
  } catch (error: any) {
    console.error(`Stream feed failed: ${error?.message ?? error}`);
    console.log("Falling back to standard /events endpoint...");

    const currentCount = await getEventCount(pool);
    if (currentCount < TARGET_EVENTS) {
      const fallbackWorker = new WorkerPool(client, pool, config, currentCount);
      await fallbackWorker.runPipelined(null);
    }
  }

  // Verify count
  const countAfterMain = await getEventCount(pool);
  if (countAfterMain < TARGET_EVENTS) {
    console.log(`After primary: ${countAfterMain}. Running fallback...`);
    const fallbackWorker = new WorkerPool(client, pool, config, countAfterMain);
    await fallbackWorker.runPipelined(null);
  }

  // Post-ingestion
  await createIndexes(pool);

  const finalCount = await getEventCount(pool);
  console.log(`Ingestion finished. Total events in DB: ${finalCount}`);

  await exportEventIds(pool, EVENT_IDS_PATH);
  console.log("ingestion complete");
}
