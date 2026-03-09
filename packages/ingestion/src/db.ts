import { Pool, PoolClient } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import { pipeline } from "stream/promises";
import { Readable } from "stream";
import { createWriteStream } from "fs";
import { Config } from "./config";
import { RawEvent, Checkpoint } from "./types";

export function createPool(config: Config): Pool {
  return new Pool({
    connectionString: config.databaseUrl,
    max: config.dbPoolSize,
  });
}

export async function runMigrations(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ingested_events (
      id           TEXT PRIMARY KEY,
      session_id   TEXT,
      type         TEXT,
      user_id      TEXT,
      timestamp    TIMESTAMPTZ,
      properties   JSONB,
      raw          JSONB,
      ingested_at  TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE UNLOGGED TABLE IF NOT EXISTS _staging (
      id           TEXT,
      session_id   TEXT,
      type         TEXT,
      user_id      TEXT,
      timestamp    TIMESTAMPTZ,
      properties   JSONB,
      raw          JSONB
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
      id            INTEGER PRIMARY KEY DEFAULT 1,
      cursor        TEXT,
      events_saved  BIGINT DEFAULT 0,
      updated_at    TIMESTAMPTZ DEFAULT NOW(),
      CONSTRAINT single_row CHECK (id = 1)
    );
  `);

  console.log("Database schema initialized.");
}

export async function createIndexes(pool: Pool): Promise<void> {
  console.log("Creating indexes (post-ingestion)...");
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_ingested_events_timestamp ON ingested_events (timestamp);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_ingested_events_session_id ON ingested_events (session_id);`);
  console.log("Indexes created.");
}

export function normalizeTimestamp(raw: unknown): string {
  if (raw === null || raw === undefined) return new Date().toISOString();
  if (typeof raw === "number") {
    const ms = raw > 1e12 ? raw : raw * 1000;
    return new Date(ms).toISOString();
  }
  if (typeof raw === "string") {
    const parsed = new Date(raw);
    if (!isNaN(parsed.getTime())) return parsed.toISOString();
    const withUtc = new Date(raw + "Z");
    if (!isNaN(withUtc.getTime())) return withUtc.toISOString();
  }
  return new Date().toISOString();
}

function escapeCopyValue(val: string | null): string {
  if (val === null) return "\\N";
  return val
    .replace(/\\/g, "\\\\")
    .replace(/\t/g, "\\t")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r");
}

// COPY into staging table via stream.pipeline(), then INSERT ... ON CONFLICT
export async function bulkInsertEvents(pool: Pool, events: RawEvent[]): Promise<number> {
  if (events.length === 0) return 0;

  const client = await pool.connect();
  try {
    await client.query("TRUNCATE _staging");

    // Build tab-separated COPY data
    const lines: string[] = [];
    for (const e of events) {
      lines.push([
        escapeCopyValue(e.id),
        escapeCopyValue(e.sessionId ?? null),
        escapeCopyValue(e.type ?? null),
        escapeCopyValue(e.userId ?? null),
        escapeCopyValue(normalizeTimestamp(e.timestamp)),
        escapeCopyValue(e.properties ? JSON.stringify(e.properties) : null),
        escapeCopyValue(JSON.stringify(e)),
      ].join("\t"));
    }
    const data = lines.join("\n") + "\n";

    // Use stream.pipeline() for proper backpressure + completion handling
    const copyStream = client.query(
      copyFrom("COPY _staging (id, session_id, type, user_id, timestamp, properties, raw) FROM STDIN")
    );
    await pipeline(Readable.from([data]), copyStream);

    // Move from staging to main table with dedup
    const result = await client.query(`
      INSERT INTO ingested_events (id, session_id, type, user_id, timestamp, properties, raw)
      SELECT id, session_id, type, user_id, timestamp, properties, raw FROM _staging
      ON CONFLICT (id) DO NOTHING
    `);

    return result.rowCount ?? 0;
  } finally {
    client.release();
  }
}

// Fallback: unnest-based bulk INSERT (no pg-copy-streams dependency)
export async function bulkInsertEventsUnnest(pool: Pool, events: RawEvent[]): Promise<number> {
  if (events.length === 0) return 0;

  const ids: string[] = [];
  const sessionIds: (string | null)[] = [];
  const types: (string | null)[] = [];
  const userIds: (string | null)[] = [];
  const timestamps: string[] = [];
  const properties: (string | null)[] = [];
  const raws: string[] = [];

  for (const e of events) {
    ids.push(e.id);
    sessionIds.push(e.sessionId ?? null);
    types.push(e.type ?? null);
    userIds.push(e.userId ?? null);
    timestamps.push(normalizeTimestamp(e.timestamp));
    properties.push(e.properties ? JSON.stringify(e.properties) : null);
    raws.push(JSON.stringify(e));
  }

  const result = await pool.query(
    `INSERT INTO ingested_events (id, session_id, type, user_id, timestamp, properties, raw)
     SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[], $6::jsonb[], $7::jsonb[])
     ON CONFLICT (id) DO NOTHING`,
    [ids, sessionIds, types, userIds, timestamps, properties, raws]
  );

  return result.rowCount ?? 0;
}

export async function saveCheckpoint(pool: Pool, cursor: string | null, eventsSaved: number): Promise<void> {
  await pool.query(
    `INSERT INTO ingestion_checkpoints (id, cursor, events_saved, updated_at)
     VALUES (1, $1, $2, NOW())
     ON CONFLICT (id) DO UPDATE SET cursor = $1, events_saved = $2, updated_at = NOW()`,
    [cursor, eventsSaved]
  );
}

export async function loadCheckpoint(pool: Pool): Promise<Checkpoint | null> {
  const result = await pool.query(
    `SELECT cursor, events_saved, updated_at FROM ingestion_checkpoints WHERE id = 1`
  );
  if (result.rows.length === 0) return null;
  const row = result.rows[0];
  return {
    cursor: row.cursor,
    eventsSaved: parseInt(row.events_saved, 10),
    updatedAt: new Date(row.updated_at),
  };
}

export async function getEventCount(pool: Pool): Promise<number> {
  const result = await pool.query(`SELECT COUNT(*)::int AS count FROM ingested_events`);
  return result.rows[0].count;
}

export async function exportEventIds(pool: Pool, filePath: string): Promise<number> {
  const client = await pool.connect();
  try {
    const stream = createWriteStream(filePath);
    const result = await client.query(`SELECT id FROM ingested_events ORDER BY id`);
    let count = 0;
    for (const row of result.rows) {
      stream.write(row.id + "\n");
      count++;
    }
    stream.end();
    await new Promise<void>((resolve, reject) => {
      stream.on("finish", resolve);
      stream.on("error", reject);
    });
    console.log(`Exported ${count} event IDs to ${filePath}`);
    return count;
  } finally {
    client.release();
  }
}
