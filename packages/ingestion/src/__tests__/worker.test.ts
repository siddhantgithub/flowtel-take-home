import { CursorExpiredError } from "../api";

const mockBulkInsert = jest.fn();
const mockBulkInsertUnnest = jest.fn();
const mockSaveCheckpoint = jest.fn();
const mockGetEventCount = jest.fn();

jest.mock("../db", () => ({
  bulkInsertEvents: (...args: any[]) => mockBulkInsert(...args),
  bulkInsertEventsUnnest: (...args: any[]) => mockBulkInsertUnnest(...args),
  saveCheckpoint: (...args: any[]) => mockSaveCheckpoint(...args),
  getEventCount: (...args: any[]) => mockGetEventCount(...args),
}));

import { WorkerPool } from "../worker";

function makeConfig() {
  return {
    apiKey: "test-key",
    apiBaseUrl: "http://localhost",
    databaseUrl: "",
    concurrency: 1,
    batchSize: 100,
    dbPoolSize: 5,
  };
}

function makeMockClient() {
  return {
    fetchFeed: jest.fn(),
    fetchEvents: jest.fn(),
    getStreamAccess: jest.fn(),
  } as any;
}

function makePage(count: number, hasMore: boolean, nextCursor: string | null = "cursor-next") {
  return {
    response: {
      data: Array.from({ length: count }, () => ({
        id: `evt-${Math.random().toString(36).slice(2)}`,
        sessionId: "sess-1",
        type: "click",
        timestamp: Date.now(),
      })),
      pagination: { limit: 100, hasMore, nextCursor, cursorExpiresIn: 120 },
      meta: { total: 3000000, returned: count, requestId: "req-1" },
    },
    rateLimit: { limit: null, remaining: null, reset: null },
  };
}

describe("WorkerPool", () => {
  const pool = {} as any;

  beforeEach(() => {
    jest.clearAllMocks();
    mockBulkInsert.mockResolvedValue(100);
    mockBulkInsertUnnest.mockResolvedValue(100);
    mockSaveCheckpoint.mockResolvedValue(undefined);
  });

  describe("runFeed - cursor expiration recovery", () => {
    it("resets cursor and continues on CursorExpiredError", async () => {
      const client = makeMockClient();
      client.fetchFeed
        .mockResolvedValueOnce(makePage(100, true, "cursor-1"))
        .mockRejectedValueOnce(new CursorExpiredError("Cursor expired"))
        .mockResolvedValueOnce(makePage(100, true, "cursor-2"))
        .mockResolvedValueOnce(makePage(100, false, null));

      mockGetEventCount.mockResolvedValue(300);

      const worker = new WorkerPool(client, pool, makeConfig(), 0);
      await worker.runFeed(null);

      expect(client.fetchFeed).toHaveBeenCalledTimes(4);
      // After expiry, third call should have cursor=undefined (reset)
      const thirdCall = client.fetchFeed.mock.calls[2];
      expect(thirdCall[0]).toBeUndefined();
    });
  });

  describe("runFeed - empty page guard", () => {
    it("stops after 10 consecutive empty pages", async () => {
      const client = makeMockClient();
      for (let i = 0; i < 10; i++) {
        client.fetchFeed.mockResolvedValueOnce(makePage(0, true, `cursor-${i}`));
      }

      mockGetEventCount.mockResolvedValue(0);

      const worker = new WorkerPool(client, pool, makeConfig(), 0);
      await worker.runFeed(null);

      expect(client.fetchFeed).toHaveBeenCalledTimes(10);
    });

    it("resets empty counter on non-empty page", async () => {
      const client = makeMockClient();
      // 5 empty, 1 with data, 10 empty (should stop)
      for (let i = 0; i < 5; i++) {
        client.fetchFeed.mockResolvedValueOnce(makePage(0, true, `c-${i}`));
      }
      client.fetchFeed.mockResolvedValueOnce(makePage(100, true, "c-data"));
      for (let i = 0; i < 10; i++) {
        client.fetchFeed.mockResolvedValueOnce(makePage(0, true, `c-e-${i}`));
      }

      mockGetEventCount.mockResolvedValue(100);

      const worker = new WorkerPool(client, pool, makeConfig(), 0);
      await worker.runFeed(null);

      // 5 empty + 1 data + 10 empty = 16
      expect(client.fetchFeed).toHaveBeenCalledTimes(16);
    });
  });

  describe("runFeed - COPY fallback to unnest", () => {
    it("switches to unnest when COPY fails", async () => {
      const client = makeMockClient();
      client.fetchFeed
        .mockResolvedValueOnce(makePage(100, true, "c1"))
        .mockResolvedValueOnce(makePage(100, false, null));

      mockBulkInsert.mockRejectedValueOnce(new Error("COPY stream error"));
      mockBulkInsertUnnest.mockResolvedValue(100);
      mockGetEventCount.mockResolvedValue(200);

      const worker = new WorkerPool(client, pool, makeConfig(), 0);
      await worker.runFeed(null);

      // COPY tried once then failed
      expect(mockBulkInsert).toHaveBeenCalledTimes(1);
      // unnest used for fallback of 1st batch + 2nd batch
      expect(mockBulkInsertUnnest).toHaveBeenCalledTimes(2);
    });
  });

  describe("runFeed - pending insert flush", () => {
    it("flushes pending insert when feed ends", async () => {
      const client = makeMockClient();
      let insertResolved = false;
      const slowInsert = new Promise<number>((resolve) => {
        setTimeout(() => { insertResolved = true; resolve(100); }, 50);
      });

      client.fetchFeed.mockResolvedValueOnce(makePage(100, false, null));
      mockBulkInsert.mockReturnValueOnce(slowInsert);
      mockGetEventCount.mockResolvedValue(100);

      const worker = new WorkerPool(client, pool, makeConfig(), 0);
      await worker.runFeed(null);

      expect(insertResolved).toBe(true);
    });
  });

  describe("runPipelined - cursor expiration recovery", () => {
    it("resets cursor and continues on CursorExpiredError", async () => {
      const client = makeMockClient();
      client.fetchEvents
        .mockResolvedValueOnce(makePage(100, true, "c1"))
        .mockRejectedValueOnce(new CursorExpiredError("expired"))
        .mockResolvedValueOnce(makePage(100, false, null));

      mockGetEventCount.mockResolvedValue(200);

      const worker = new WorkerPool(client, pool, makeConfig(), 0);
      await worker.runPipelined(null);

      expect(client.fetchEvents).toHaveBeenCalledTimes(3);
      // After expiry, cursor should be reset
      const thirdCall = client.fetchEvents.mock.calls[2];
      expect(thirdCall[0]).toBeUndefined();
    });
  });
});
