import { Config } from "./config";
import { ApiResponse, SessionsApiResponse } from "./types";

export interface RateLimitInfo {
  limit: number | null;
  remaining: number | null;
  reset: number | null;
}

export class RateLimitError extends Error {
  retryAfter: number;
  rateLimit: RateLimitInfo;

  constructor(retryAfter: number, rateLimit: RateLimitInfo) {
    super(`Rate limited. Retry after ${retryAfter}s`);
    this.name = "RateLimitError";
    this.retryAfter = retryAfter;
    this.rateLimit = rateLimit;
  }
}

const NON_RETRYABLE_STATUSES = [401, 403];
const MAX_RETRIES = 5;
const BROWSER_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36";

export interface StreamAccess {
  endpoint: string;
  token: string;
  expiresIn: number;
  obtainedAt: number;
}

export class ApiClient {
  private apiKey: string;
  private baseUrl: string;
  private firstCallPerEndpoint: Set<string> = new Set();
  private lastRequestTime: Map<string, number> = new Map();
  private rateLimitInfo: Map<string, { limit: number; reset: number }> = new Map();

  // Stream access
  private streamAccess: StreamAccess | null = null;

  constructor(config: Config) {
    this.apiKey = config.apiKey;
    this.baseUrl = config.apiBaseUrl;
  }

  // Obtain a stream access token (valid for ~5 min)
  async getStreamAccess(): Promise<StreamAccess> {
    // Return cached if still valid (refresh 60s before expiry)
    if (this.streamAccess) {
      const elapsed = (Date.now() - this.streamAccess.obtainedAt) / 1000;
      if (elapsed < this.streamAccess.expiresIn - 60) {
        return this.streamAccess;
      }
      console.log("Stream token expiring soon, refreshing...");
    }

    console.log("Obtaining stream access token...");
    let lastErr: Error | null = null;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const res = await fetch(`${this.baseUrl}/internal/dashboard/stream-access`, {
          method: "POST",
          headers: {
            "X-API-Key": this.apiKey,
            "Content-Type": "application/json",
            "Cookie": `dashboard_api_key=${this.apiKey}`,
            "User-Agent": BROWSER_UA,
            "Referer": `${this.baseUrl}/`,
            "Origin": this.baseUrl,
          },
        });

        if (!res.ok) {
          const body = await res.text();
          throw new Error(`Failed to get stream access: ${res.status} ${body}`);
        }

        const data = await res.json() as any;
        const sa = data.streamAccess;

    this.streamAccess = {
      endpoint: sa.endpoint,
      token: sa.token,
      expiresIn: sa.expiresIn,
      obtainedAt: Date.now(),
    };

        console.log(`Stream token obtained. Endpoint: ${sa.endpoint}, expires in ${sa.expiresIn}s`);
        return this.streamAccess;
      } catch (err: any) {
        lastErr = err;
        if (attempt < 3) {
          const backoff = 1000 * Math.pow(2, attempt - 1);
          console.log(`[StreamAccess attempt ${attempt}/3] ${err.message}. Retrying in ${backoff}ms...`);
          await sleep(backoff);
        }
      }
    }
    throw lastErr ?? new Error("Failed to get stream access after retries");
  }

  // Fetch from the stream/feed endpoint (no rate limit!)
  async fetchFeed(cursor?: string | null, limit?: number, since?: number, until?: number): Promise<{ response: ApiResponse; rateLimit: RateLimitInfo }> {
    const stream = await this.getStreamAccess();

    const url = new URL(`${this.baseUrl}${stream.endpoint}`);
    if (limit) url.searchParams.set("limit", String(limit));
    if (cursor) url.searchParams.set("cursor", cursor);
    if (since) url.searchParams.set("since", String(since));
    if (until) url.searchParams.set("until", String(until));

    let lastError: Error | null = null;
    const maxAttempts = 20; // more retries for rate limits
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        const res = await fetch(url.toString(), {
          headers: {
            "X-API-Key": this.apiKey,
            "X-Stream-Token": stream.token,
            "Accept": "application/json",
          },
        });

        if (!this.firstCallPerEndpoint.has("feed")) {
          console.log("[Headers feed]", Object.fromEntries(res.headers.entries()));
          this.firstCallPerEndpoint.add("feed");
        }

        if (res.status === 403) {
          // Token might have expired — refresh and retry
          const body = await res.text();
          if (body.includes("expired") || body.includes("INVALID_STREAM_TOKEN")) {
            console.log("Stream token expired, refreshing...");
            this.streamAccess = null;
            await this.getStreamAccess();
            continue;
          }
          throw new Error(`Feed forbidden: ${body}`);
        }

        if (res.status === 429) {
          const retryAfter = parseIntHeader(res.headers, "retry-after") ?? 5;
          lastError = new Error(`Feed rate limited after ${attempt} attempts`);
          console.log(`Feed rate limited (attempt ${attempt}/${maxAttempts}), waiting ${retryAfter}s...`);
          await sleep(retryAfter * 1000);
          continue;
        }

        if (!res.ok) {
          const body = await res.text();
          throw new Error(`Feed error ${res.status}: ${body}`);
        }

        const body = await res.json() as any;
        const rateLimit: RateLimitInfo = { limit: null, remaining: null, reset: null };

        const response: ApiResponse = {
          data: body.data ?? [],
          pagination: {
            limit: body.pagination?.limit ?? limit ?? 5000,
            hasMore: body.pagination?.hasMore ?? false,
            nextCursor: body.pagination?.nextCursor ?? null,
            cursorExpiresIn: body.pagination?.cursorExpiresIn ?? null,
          },
          meta: {
            total: body.meta?.total ?? 0,
            returned: body.meta?.returned ?? (body.data?.length ?? 0),
            requestId: body.meta?.requestId ?? "",
          },
        };

        return { response, rateLimit };
      } catch (error: any) {
        lastError = error;
        if (attempt < maxAttempts) {
          const backoff = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          console.log(`[Feed attempt ${attempt}/${maxAttempts}] ${error.message}. Retrying in ${backoff}ms...`);
          await sleep(backoff);
        }
      }
    }

    throw lastError ?? new Error("Feed failed after max retries");
  }

  // Fetch events page from /api/v1/events (rate-limited fallback)
  async fetchEvents(cursor?: string | null, limit?: number): Promise<{ response: ApiResponse; rateLimit: RateLimitInfo }> {
    return this.fetchWithRetry("/api/v1/events", cursor, limit);
  }

  // Space requests evenly within the rate limit window
  private async paceRequest(bucket: string): Promise<void> {
    const info = this.rateLimitInfo.get(bucket);
    if (!info) return;
    const minIntervalMs = (info.reset * 1000) / info.limit;
    const lastTime = this.lastRequestTime.get(bucket) ?? 0;
    const elapsed = Date.now() - lastTime;
    const waitMs = minIntervalMs - elapsed;
    if (waitMs > 0) {
      await sleep(waitMs);
    }
    this.lastRequestTime.set(bucket, Date.now());
  }

  private async fetchWithRetry(path: string, cursor?: string | null, limit?: number, extraParams?: Record<string, string>): Promise<{ response: ApiResponse; rateLimit: RateLimitInfo }> {
    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        return await this._fetch(path, cursor, limit, extraParams);
      } catch (error: any) {
        lastError = error;
        if (error instanceof RateLimitError) {
          console.log(`[Attempt ${attempt}/${MAX_RETRIES}] Rate limited on ${path}, waiting ${error.retryAfter}s...`);
          await sleep(error.retryAfter * 1000);
          continue;
        }
        if (error.nonRetryable) throw error;
        if (attempt < MAX_RETRIES) {
          const backoff = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          console.log(`[Attempt ${attempt}/${MAX_RETRIES}] ${error.message}. Retrying in ${backoff}ms...`);
          await sleep(backoff);
        }
      }
    }
    throw lastError;
  }

  private async _fetch(path: string, cursor?: string | null, limit?: number, extraParams?: Record<string, string>): Promise<{ response: ApiResponse; rateLimit: RateLimitInfo }> {
    const bucket = path.replace(/\/[0-9a-f-]{36}/g, "/:id");
    await this.paceRequest(bucket);

    const url = new URL(`${this.baseUrl}${path}`);
    if (limit) url.searchParams.set("limit", String(limit));
    if (cursor) url.searchParams.set("cursor", cursor);
    if (extraParams) {
      for (const [k, v] of Object.entries(extraParams)) {
        url.searchParams.set(k, v);
      }
    }

    const res = await fetch(url.toString(), {
      headers: { "X-API-Key": this.apiKey, "Accept": "application/json" },
    });

    if (!this.firstCallPerEndpoint.has(path)) {
      console.log(`[Headers ${path}]`, Object.fromEntries(res.headers.entries()));
      this.firstCallPerEndpoint.add(path);
    }

    const rateLimit = this.parseRateLimitHeaders(res.headers);

    if (res.status === 429) {
      const retryAfter = parseIntHeader(res.headers, "retry-after") ?? Math.ceil((rateLimit.reset ?? 60));
      throw new RateLimitError(retryAfter, rateLimit);
    }

    if (NON_RETRYABLE_STATUSES.includes(res.status)) {
      const body = await res.text();
      const err = new Error(`API error ${res.status} on ${path}: ${body}`) as any;
      err.nonRetryable = true;
      throw err;
    }

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`API error ${res.status} on ${path}: ${body}`);
    }

    if (rateLimit.limit && rateLimit.reset) {
      this.rateLimitInfo.set(bucket, { limit: rateLimit.limit, reset: rateLimit.reset });
    }

    const body = await res.json() as any;

    const response: ApiResponse = {
      data: body.data ?? [],
      pagination: {
        limit: body.pagination?.limit ?? limit ?? 1000,
        hasMore: body.pagination?.hasMore ?? false,
        nextCursor: body.pagination?.nextCursor ?? null,
        cursorExpiresIn: body.pagination?.cursorExpiresIn ?? null,
      },
      meta: {
        total: body.meta?.total ?? 0,
        returned: body.meta?.returned ?? (body.data?.length ?? 0),
        requestId: body.meta?.requestId ?? "",
      },
    };

    return { response, rateLimit };
  }

  private parseRateLimitHeaders(headers: Headers): RateLimitInfo {
    return {
      limit: parseIntHeader(headers, "x-ratelimit-limit"),
      remaining: parseIntHeader(headers, "x-ratelimit-remaining"),
      reset: parseIntHeader(headers, "x-ratelimit-reset"),
    };
  }
}

function parseIntHeader(headers: Headers, name: string): number | null {
  const value = headers.get(name);
  if (value === null) return null;
  const parsed = parseInt(value, 10);
  return isNaN(parsed) ? null : parsed;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
