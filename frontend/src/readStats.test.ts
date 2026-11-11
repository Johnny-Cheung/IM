import { describe, expect, it } from "vitest";
import {
  ReadStatsGate,
  parseGroupReadStats,
} from "./readStats";

describe("group read stats", () => {
  it("parses a valid response", () => {
    expect(parseGroupReadStats({
      seq: 12,
      message_id: 345,
      read_count: 3,
      member_count: 5,
      all_read: false,
    })).toEqual({
      seq: 12,
      message_id: 345,
      read_count: 3,
      member_count: 5,
      all_read: false,
    });
  });

  it("drops malformed responses", () => {
    expect(parseGroupReadStats(null)).toBeNull();
    expect(parseGroupReadStats({ message_id: 1 })).toBeNull();
    expect(parseGroupReadStats({
      message_id: 1,
      read_count: 0,
      member_count: 2,
      all_read: "no",
    })).toBeNull();
    expect(parseGroupReadStats({
      seq: 0,
      message_id: 1,
      read_count: 0,
      member_count: 2,
      all_read: false,
    })).toBeNull();
    expect(parseGroupReadStats({
      seq: 1.5,
      message_id: 1,
      read_count: 0,
      member_count: 2,
      all_read: false,
    })).toBeNull();
  });
});

describe("ReadStatsGate", () => {
  it("accepts only the newest request for the current key", () => {
    const gate = new ReadStatsGate();
    const first = gate.begin(7, 10);
    const second = gate.begin(7, 11);
    expect(gate.accepts(first, 7, 10)).toBe(false);
    expect(gate.accepts(second, 7, 11)).toBe(true);
    expect(gate.accepts(second, 7, 10)).toBe(false);
    expect(gate.accepts(second, 8, 11)).toBe(false);
  });

  it("rejects responses after a conversation switch", () => {
    const gate = new ReadStatsGate();
    const token = gate.begin(7, 10);
    gate.reset();
    expect(gate.accepts(token, 7, 10)).toBe(false);
    const next = gate.begin(8, 10);
    expect(gate.accepts(next, 8, 10)).toBe(true);
  });
});
