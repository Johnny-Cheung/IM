import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { GroupReadStats } from "./api";
import {
  GROUP_RECEIPT_MAX_INTERVAL_MS,
  GROUP_RECEIPT_STOP_AFTER_MS,
  GroupReceiptPoller,
  groupReceiptDelay,
  withJitter,
} from "./groupReceiptPolling";

const allRead: GroupReadStats = {
  seq: 12,
  message_id: 345,
  read_count: 2,
  member_count: 2,
  all_read: true,
};

describe("groupReceiptDelay", () => {
  it("polls recent receipts quickly and backs off with age", () => {
    expect(groupReceiptDelay(0, 0, 0)).toBe(1000);
    expect(groupReceiptDelay(30_000, 0, 0)).toBe(3000);
    expect(groupReceiptDelay(2 * 60_000, 0, 0)).toBe(10_000);
    expect(groupReceiptDelay(6 * 60_000, 0, 0)).toBe(30_000);
  });

  it("backs off further when nothing changes", () => {
    expect(groupReceiptDelay(30_000, 0, 4)).toBe(12_000);
    expect(groupReceiptDelay(2 * 60_000, 0, 10)).toBe(GROUP_RECEIPT_MAX_INTERVAL_MS);
  });

  it("speeds up after a change", () => {
    expect(groupReceiptDelay(2 * 60_000, 5, 0)).toBe(1000);
  });

  it("stops after the maximum age unless a change is still being chased", () => {
    expect(groupReceiptDelay(GROUP_RECEIPT_STOP_AFTER_MS, 0, 0)).toBeNull();
    expect(groupReceiptDelay(GROUP_RECEIPT_STOP_AFTER_MS, 1, 0)).toBe(1000);
  });
});

describe("withJitter", () => {
  it("keeps the interval within twenty percent", () => {
    expect(withJitter(1000, () => 0)).toBe(850);
    expect(withJitter(1000, () => 0.5)).toBe(1000);
    expect(withJitter(1000, () => 1)).toBe(1150);
  });
});

describe("GroupReceiptPoller", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  async function flushMicrotasks() {
    await Promise.resolve();
    await Promise.resolve();
  }

  it("polls immediately and stops once every member has read", async () => {
    const fetch = vi.fn(async () => allRead);
    const poller = new GroupReceiptPoller({
      isEligible: () => true,
      getAgeMs: () => 0,
      fetch,
      random: () => 0.5,
    });

    poller.start(true, true);
    vi.advanceTimersByTime(0);
    await flushMicrotasks();
    expect(fetch).toHaveBeenCalledTimes(1);

    vi.advanceTimersByTime(1000);
    await flushMicrotasks();
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("does not poll while the receipt is off screen", async () => {
    const fetch = vi.fn(async () => allRead);
    const poller = new GroupReceiptPoller({
      isEligible: () => false,
      getAgeMs: () => 0,
      fetch,
      random: () => 0.5,
    });

    poller.start(true, true);
    vi.advanceTimersByTime(5000);
    await flushMicrotasks();
    expect(fetch).not.toHaveBeenCalled();
  });

  it("stops when the receipt leaves the viewport", async () => {
    let eligible = true;
    const fetch = vi.fn(async () => null);
    const poller = new GroupReceiptPoller({
      isEligible: () => eligible,
      getAgeMs: () => 0,
      fetch,
      random: () => 0.5,
    });

    poller.start(true, true);
    vi.advanceTimersByTime(0);
    await flushMicrotasks();
    expect(fetch).toHaveBeenCalledTimes(1);

    eligible = false;
    vi.advanceTimersByTime(1000);
    await flushMicrotasks();
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("refreshes immediately when a visible target changes", async () => {
    let active = false;
    const fetch = vi.fn(async () => null);
    const poller = new GroupReceiptPoller({
      isEligible: () => active,
      getAgeMs: () => 0,
      fetch,
      random: () => 0.5,
    });

    poller.start(true, true);
    vi.advanceTimersByTime(1000);
    await flushMicrotasks();
    expect(fetch).not.toHaveBeenCalled();

    active = true;
    poller.sync();
    vi.advanceTimersByTime(0);
    await flushMicrotasks();
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});
