import { CursorExpiredError } from "../api";

// Mock fetch globally
const mockFetch = jest.fn();
(global as any).fetch = mockFetch;

// Import after mocking fetch
import { ApiClient } from "../api";

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

describe("CursorExpiredError", () => {
  it("is an instance of Error", () => {
    const err = new CursorExpiredError("test");
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("CursorExpiredError");
  });
});

describe("ApiClient cursor expiration", () => {
  let client: ApiClient;

  beforeEach(() => {
    mockFetch.mockReset();
    client = new ApiClient(makeConfig());
  });

  it("throws CursorExpiredError on 400 with CURSOR_EXPIRED from /events", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      headers: new Headers(),
      text: async () => '{"error":"Bad Request","message":"Cursor expired 3 minute(s) ago","code":"CURSOR_EXPIRED"}',
    });

    await expect(
      client.fetchEvents("expired-cursor", 100)
    ).rejects.toThrow(CursorExpiredError);
  });

  it("does NOT retry CursorExpiredError — throws immediately", async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 400,
      headers: new Headers(),
      text: async () => '{"code":"CURSOR_EXPIRED"}',
    });

    await expect(
      client.fetchEvents("expired-cursor", 100)
    ).rejects.toThrow(CursorExpiredError);

    // Should only call fetch once — no retries
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("throws CursorExpiredError on 400 with CURSOR_EXPIRED from feed", async () => {
    // First call: getStreamAccess
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        streamAccess: {
          endpoint: "/api/v1/events/feed",
          token: "test-token",
          expiresIn: 300,
        },
      }),
    });
    // Second call: fetchFeed returns cursor expired
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      headers: new Headers(),
      text: async () => '{"code":"CURSOR_EXPIRED","message":"Cursor expired"}',
    });

    await expect(
      client.fetchFeed("expired-cursor", 100)
    ).rejects.toThrow(CursorExpiredError);
  });
});
