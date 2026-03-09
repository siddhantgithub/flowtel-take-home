export interface Config {
  apiKey: string;
  apiBaseUrl: string;
  databaseUrl: string;
  concurrency: number;
  batchSize: number;
  dbPoolSize: number;
}

export function loadConfig(): Config {
  const apiKey = process.env.API_KEY || process.env.TARGET_API_KEY;
  if (!apiKey) {
    throw new Error("Missing required env var: API_KEY or TARGET_API_KEY");
  }

  const apiBaseUrl = process.env.API_BASE_URL;
  if (!apiBaseUrl) {
    throw new Error("Missing required env var: API_BASE_URL");
  }

  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("Missing required env var: DATABASE_URL");
  }

  return {
    apiKey,
    apiBaseUrl,
    databaseUrl,
    concurrency: parseInt(process.env.CONCURRENCY || "10", 10),
    batchSize: parseInt(process.env.BATCH_SIZE || "1000", 10),
    dbPoolSize: parseInt(process.env.DB_POOL_SIZE || "20", 10),
  };
}
