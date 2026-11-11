import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ReadCursorReporter,
  lastVisibleReadSeq,
  readFrame,
  readTargetKey,
  type ReadCursorReporterOptions,
} from "./readCursor";

describe("read frames", () => {
  it("builds a direct read frame and trims the username", () => {
    expect(readFrame({ kind: "direct", username: " bob " }, 42)).toEqual({
      type: "read",
      kind: "direct",
      username: "bob",
      last_read_seq: 42,
    });
  });

  it("builds a group read frame", () => {
    expect(readFrame({ kind: "group", groupID: 9 }, 128)).toEqual({
      type: "read",
      kind: "group",
      group_id: 9,
      last_read_seq: 128,
    });
  });

  it("rejects zero, negative and unsafe positions", () => {
    const target = { kind: "direct" as const, username: "bob" };
    expect(readFrame(target, 0)).toBeNull();
    expect(readFrame(target, -1)).toBeNull();
    expect(readFrame(target, 1.5)).toBeNull();
    expect(readFrame(target, Number.MAX_SAFE_INTEGER + 1)).toBeNull();
  });

  it("rejects empty targets", () => {
    expect(readFrame({ kind: "direct", username: "  " }, 1)).toBeNull();
    expect(readFrame({ kind: "group", groupID: 0 }, 1)).toBeNull();
  });

  it("keys targets case-insensitively", () => {
    expect(readTargetKey({ kind: "direct", username: "Bob" })).toBe("direct:bob");
    expect(readTargetKey({ kind: "group", groupID: 7 })).toBe("group:7");
  });
});

describe("lastVisibleReadSeq", () => {
  const viewport = { top: 0, bottom: 100 };

  it("returns the highest intersecting row", () => {
    expect(lastVisibleReadSeq(viewport, [
      { seq: 1, top: 10, bottom: 20 },
      { seq: 2, top: 50, bottom: 60 },
      { seq: 3, top: 110, bottom: 120 },
    ])).toBe(2);
  });

  it("counts a partially visible row", () => {
    expect(lastVisibleReadSeq(viewport, [
      { seq: 4, top: 90, bottom: 110 },
    ])).toBe(4);
  });

  it("ignores rows entirely above or below the viewport", () => {
    expect(lastVisibleReadSeq(viewport, [
      { seq: 1, top: -20, bottom: 0 },
      { seq: 2, top: 100, bottom: 120 },
    ])).toBe(0);
  });

  it("ignores invalid seqs and empty input", () => {
    expect(lastVisibleReadSeq(viewport, [
      { seq: 0, top: 10, bottom: 20 },
      { seq: Number.NaN, top: 20, bottom: 30 },
      { seq: 5, top: 40, bottom: 50 },
    ])).toBe(5);
    expect(lastVisibleReadSeq(viewport, [])).toBe(0);
  });
});

describe("ReadCursorReporter", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function newReporter(
    result = true,
    options: ReadCursorReporterOptions = {
      directLeading: false,
      directIntervalMs: 1000,
    },
  ) {
    const send = vi.fn(() => result);
    const reporter = new ReadCursorReporter(send, options);
    reporter.setTarget({ kind: "direct", username: "bob" });
    return { reporter, send };
  }

  it("sends the first direct observation immediately and coalesces the next window", () => {
    const send = vi.fn(() => true);
    const reporter = new ReadCursorReporter(send);
    reporter.setTarget({ kind: "direct", username: "bob" });

    reporter.observe(5);
    expect(send).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledWith({
      type: "read",
      kind: "direct",
      username: "bob",
      last_read_seq: 5,
    });

    reporter.observe(6);
    reporter.observe(8);
    expect(send).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(199);
    expect(send).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1);
    expect(send).toHaveBeenCalledTimes(2);
    expect(send).toHaveBeenLastCalledWith({
      type: "read",
      kind: "direct",
      username: "bob",
      last_read_seq: 8,
    });
  });

  it("keeps group reporting on the one-second window", () => {
    const send = vi.fn(() => true);
    const reporter = new ReadCursorReporter(send);
    reporter.setTarget({ kind: "group", groupID: 7 });

    reporter.observe(12);
    expect(send).not.toHaveBeenCalled();
    vi.advanceTimersByTime(999);
    expect(send).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(send).toHaveBeenCalledWith({
      type: "read",
      kind: "group",
      group_id: 7,
      last_read_seq: 12,
    });
  });

  it("retries a rejected leading direct frame after the short interval", () => {
    const send = vi.fn(() => false);
    const reporter = new ReadCursorReporter(send);
    reporter.setTarget({ kind: "direct", username: "bob" });

    reporter.observe(5);
    expect(send).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(200);
    expect(send).toHaveBeenCalledTimes(2);
  });

  it("coalesces observations into one trailing frame", () => {
    const { reporter, send } = newReporter();
    reporter.observe(5);
    reporter.observe(8);
    reporter.observe(6);
    expect(send).not.toHaveBeenCalled();
    vi.advanceTimersByTime(999);
    expect(send).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(send).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledWith({
      type: "read",
      kind: "direct",
      username: "bob",
      last_read_seq: 8,
    });
  });

  it("does not resend a position already sent", () => {
    const { reporter, send } = newReporter();
    reporter.observe(8);
    vi.advanceTimersByTime(1000);
    reporter.observe(8);
    vi.advanceTimersByTime(1000);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("flushes immediately on demand", () => {
    const { reporter, send } = newReporter();
    reporter.observe(7);
    reporter.flushNow();
    expect(send).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1000);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("flushes the old target before switching", () => {
    const { reporter, send } = newReporter();
    reporter.observe(5);
    const changed = reporter.setTarget({ kind: "direct", username: "carol" });
    expect(changed).toBe(true);
    expect(send).toHaveBeenCalledWith({
      type: "read",
      kind: "direct",
      username: "bob",
      last_read_seq: 5,
    });
    reporter.observe(3);
    vi.advanceTimersByTime(1000);
    expect(send).toHaveBeenLastCalledWith({
      type: "read",
      kind: "direct",
      username: "carol",
      last_read_seq: 3,
    });
  });

  it("keeps the same-target state without flushing", () => {
    const { reporter, send } = newReporter();
    reporter.observe(5);
    const changed = reporter.setTarget({ kind: "direct", username: "BOB" });
    expect(changed).toBe(false);
    expect(send).not.toHaveBeenCalled();
  });

  it("does not send while paused and flushes after resume", () => {
    const { reporter, send } = newReporter();
    reporter.observe(4);
    reporter.pause();
    vi.advanceTimersByTime(1000);
    expect(send).not.toHaveBeenCalled();
    reporter.resume();
    reporter.flushNow();
    expect(send).toHaveBeenCalledWith({
      type: "read",
      kind: "direct",
      username: "bob",
      last_read_seq: 4,
    });
  });

  it("retains the position when the transport rejects the frame", () => {
    const { reporter, send } = newReporter(false);
    reporter.observe(6);
    vi.advanceTimersByTime(1000);
    expect(send).toHaveBeenCalledTimes(1);
    reporter.flushNow();
    expect(send).toHaveBeenCalledTimes(2);
    send.mockReturnValue(true);
    reporter.flushNow();
    expect(send).toHaveBeenCalledTimes(3);
    reporter.observe(6);
    vi.advanceTimersByTime(1000);
    expect(send).toHaveBeenCalledTimes(3);
  });

  it("suppresses positions covered by a server acknowledgement", () => {
    const { reporter, send } = newReporter();
    reporter.observe(5);
    reporter.noteServerPosition(10);
    vi.advanceTimersByTime(1000);
    expect(send).not.toHaveBeenCalled();
    reporter.observe(8);
    vi.advanceTimersByTime(1000);
    expect(send).not.toHaveBeenCalled();
    reporter.observe(11);
    vi.advanceTimersByTime(1000);
    expect(send).toHaveBeenCalledWith({
      type: "read",
      kind: "direct",
      username: "bob",
      last_read_seq: 11,
    });
  });

  it("replays the newest observed position after a reconnect", () => {
    const { reporter, send } = newReporter();
    reporter.observe(5);
    vi.advanceTimersByTime(1000);
    expect(send).toHaveBeenCalledTimes(1);
    reporter.observe(7);
    reporter.transportClosed();
    vi.advanceTimersByTime(1000);
    expect(send).toHaveBeenCalledTimes(1);
    reporter.resume();
    reporter.flushNow();
    expect(send).toHaveBeenCalledTimes(2);
    expect(send).toHaveBeenLastCalledWith({
      type: "read",
      kind: "direct",
      username: "bob",
      last_read_seq: 7,
    });
  });

  it("stops without sending", () => {
    const { reporter, send } = newReporter();
    reporter.observe(5);
    reporter.stop();
    reporter.resume();
    reporter.flushNow();
    vi.advanceTimersByTime(1000);
    expect(send).not.toHaveBeenCalled();
  });
});
