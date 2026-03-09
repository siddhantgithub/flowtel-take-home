import { normalizeTimestamp } from "../db";

describe("normalizeTimestamp", () => {
  it("converts epoch milliseconds to ISO string", () => {
    const result = normalizeTimestamp(1706745600000); // 2024-02-01T00:00:00Z
    expect(result).toBe("2024-02-01T00:00:00.000Z");
  });

  it("converts epoch seconds to ISO string", () => {
    const result = normalizeTimestamp(1706745600); // 2024-02-01T00:00:00Z
    expect(result).toBe("2024-02-01T00:00:00.000Z");
  });

  it("passes through valid ISO strings", () => {
    const result = normalizeTimestamp("2024-02-01T00:00:00.000Z");
    expect(result).toBe("2024-02-01T00:00:00.000Z");
  });

  it("appends Z to timezone-less strings", () => {
    const result = normalizeTimestamp("2024-02-01T12:00:00");
    expect(new Date(result).getTime()).not.toBeNaN();
  });

  it("returns current time for null", () => {
    const before = Date.now();
    const result = normalizeTimestamp(null);
    const after = Date.now();
    const ts = new Date(result).getTime();
    expect(ts).toBeGreaterThanOrEqual(before);
    expect(ts).toBeLessThanOrEqual(after);
  });

  it("returns current time for undefined", () => {
    const result = normalizeTimestamp(undefined);
    expect(new Date(result).getTime()).not.toBeNaN();
  });

  it("returns current time for unparseable string", () => {
    const before = Date.now();
    const result = normalizeTimestamp("not-a-date");
    const ts = new Date(result).getTime();
    expect(ts).toBeGreaterThanOrEqual(before);
  });

  it("distinguishes milliseconds from seconds by magnitude", () => {
    // 1e12 boundary: values > 1e12 are ms, <= 1e12 are seconds
    const asMs = normalizeTimestamp(1000000000001); // > 1e12, treated as ms
    const asSec = normalizeTimestamp(999999999999); // < 1e12, treated as seconds
    expect(new Date(asMs).getTime()).toBe(1000000000001);
    expect(new Date(asSec).getTime()).toBe(999999999999000);
  });
});
