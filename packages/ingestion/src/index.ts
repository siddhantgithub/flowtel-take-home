import { loadConfig } from "./config";
import { createPool, runMigrations } from "./db";
import { ApiClient } from "./api";
import { runIngestion } from "./coordinator";

async function main(): Promise<void> {
  const config = loadConfig();

  console.log("=========================================");
  console.log("  DataSync Ingestion Service");
  console.log(`  Started at: ${new Date().toISOString()}`);
  console.log(`  API Base: ${config.apiBaseUrl}`);
  console.log(`  API Key: ${config.apiKey.slice(0, 4)}...${config.apiKey.slice(-4)}`);
  console.log(`  Concurrency: ${config.concurrency}`);
  console.log(`  Batch Size: ${config.batchSize}`);
  console.log("=========================================");

  const pool = createPool(config);

  // Wait for DB to be ready
  const maxRetries = 30;
  for (let i = 0; i < maxRetries; i++) {
    try {
      await pool.query("SELECT 1");
      console.log("Database connected.");
      break;
    } catch {
      if (i === maxRetries - 1) throw new Error("Could not connect to database");
      console.log(`Waiting for database... (${i + 1}/${maxRetries})`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  await runMigrations(pool);

  const client = new ApiClient(config);
  const startTime = Date.now();

  await runIngestion(client, pool, config);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`Total time: ${elapsed}s`);

  await pool.end();
}

process.on("unhandledRejection", (err) => {
  console.error("Unhandled rejection:", err);
  process.exit(1);
});

process.on("uncaughtException", (err) => {
  console.error("Uncaught exception:", err);
  process.exit(1);
});

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
