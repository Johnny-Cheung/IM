import { describe, expect, it } from "vitest";
import {
  acceptedSeqMap,
  isAcceptedMessageSeq,
  isReadByPeer,
  lostOutboxEntries,
  maxAcceptedSeq,
  MissingSeqTracker,
  missingSeqs,
  nextUnread,
  outboxDisplayStatus,
  planActiveSync,
  planGapPull,
  planIncoming,
  planListRollback,
  planRollback,
  shouldLoadNewestPage,
  shouldReplacePreview,
  sortConversationMessages,
} from "./seqSync";

describe("accepted seqs", () => {
  it("accepts only integers from 1 up", () => {
    expect(isAcceptedMessageSeq(1)).toBe(true);
    expect(isAcceptedMessageSeq(0)).toBe(false);
    expect(isAcceptedMessageSeq(-1)).toBe(false);
    expect(isAcceptedMessageSeq(1.5)).toBe(false);
    expect(isAcceptedMessageSeq(Number.NaN)).toBe(false);
    expect(isAcceptedMessageSeq(Number.MAX_SAFE_INTEGER + 1)).toBe(false);
  });

  it("ignores local pending messages when computing the watermark", () => {
    const messages = [
      { seq: 0, id: 0 },
      { seq: 3, id: 30 },
      { seq: 7, id: 70 },
      { seq: 0, id: 0 },
    ];
    expect(maxAcceptedSeq(messages)).toBe(7);
    expect(maxAcceptedSeq([])).toBe(0);
  });

  it("maps seq to the global id it was seen with", () => {
    const map = acceptedSeqMap([
      { seq: 2, id: 20 },
      { seq: 0, id: 0 },
      { seq: 5, id: 50 },
    ]);
    expect(map.get(2)).toBe(20);
    expect(map.get(5)).toBe(50);
    expect(map.has(0)).toBe(false);
  });
});

describe("planIncoming", () => {
  it("appends when the seq follows the watermark", () => {
    expect(planIncoming(1, 10, { maxSeq: 0 })).toEqual({ kind: "append" });
    expect(planIncoming(8, 80, { maxSeq: 7 })).toEqual({ kind: "append" });
  });

  it("asks for the missing range when the seq jumps ahead", () => {
    expect(planIncoming(5, 50, { maxSeq: 0 })).toEqual({
      kind: "pull",
      afterSeq: 0,
      targetSeq: 5,
    });
    expect(planIncoming(9, 90, { maxSeq: 5 })).toEqual({
      kind: "pull",
      afterSeq: 5,
      targetSeq: 9,
    });
  });

  it("ignores a duplicate below the watermark", () => {
    const seqIDs = new Map([[3, 30]]);
    expect(planIncoming(3, 30, { maxSeq: 7, seqIDs }))
      .toEqual({ kind: "duplicate" });
    // Without a cached id the message still counts as already seen.
    expect(planIncoming(2, 20, { maxSeq: 7 }))
      .toEqual({ kind: "duplicate" });
  });

  it("resyncs the tail when the server reuses a seq for another message", () => {
    const seqIDs = new Map([[3, 30]]);
    expect(planIncoming(3, 99, { maxSeq: 7, seqIDs }))
      .toEqual({ kind: "resync", fromSeq: 3 });
    expect(planIncoming(7, 99, { maxSeq: 7, seqIDs: new Map([[7, 70]]) }))
      .toEqual({ kind: "resync", fromSeq: 7 });
  });

  it("ignores an invalid accepted seq", () => {
    expect(planIncoming(0, 0, { maxSeq: 4 })).toEqual({ kind: "duplicate" });
    expect(planIncoming(5, 0, { maxSeq: 4 })).toEqual({ kind: "duplicate" });
  });
});

describe("planRollback", () => {
  it("drops the local tail above the server position", () => {
    expect(planRollback(5, 8)).toEqual({ fromSeq: 6 });
    expect(planRollback(0, 3)).toEqual({ fromSeq: 1 });
  });

  it("does nothing when the server is at or ahead of the local watermark", () => {
    expect(planRollback(8, 8)).toBeNull();
    expect(planRollback(9, 8)).toBeNull();
    expect(planRollback(0, 0)).toBeNull();
  });
});

describe("planListRollback", () => {
  it("never treats a conversation missing from the list as a rollback", () => {
    expect(planListRollback(undefined, 200)).toBeNull();
    expect(planListRollback(null, 200)).toBeNull();
    expect(planListRollback(Number.NaN, 200)).toBeNull();
    expect(planListRollback(-1, 200)).toBeNull();
  });

  it("still drops the tail for an explicit server position", () => {
    expect(planListRollback(0, 200)).toEqual({ fromSeq: 1 });
    expect(planListRollback(5, 8)).toEqual({ fromSeq: 6 });
    expect(planListRollback(8, 8)).toBeNull();
    expect(planListRollback(9, 8)).toBeNull();
  });
});

describe("planActiveSync", () => {
  it("loads the newest page when the tab holds nothing", () => {
    expect(planActiveSync(0, undefined, undefined)).toEqual({ kind: "initial" });
    expect(planActiveSync(0, { id: 10, seq: 0 }, { id: 5, seq: 5 }))
      .toEqual({ kind: "initial" });
  });

  it("catches up instead of rolling back when the list snapshot lags", () => {
    // The list still says 50 while this tab already pulled through 55.
    expect(planActiveSync(55, { id: 550, seq: 55 }, { id: 500, seq: 50 }))
      .toEqual({ kind: "catch-up", afterSeq: 55 });
    expect(planActiveSync(55, { id: 550, seq: 55 }, undefined))
      .toEqual({ kind: "catch-up", afterSeq: 55 });
  });

  it("resyncs when the snapshot shows the tail seq under another id", () => {
    expect(planActiveSync(55, { id: 550, seq: 55 }, { id: 999, seq: 55 }))
      .toEqual({ kind: "resync", fromSeq: 55 });
    expect(planActiveSync(55, { id: 550, seq: 55 }, { id: 550, seq: 55 }))
      .toEqual({ kind: "catch-up", afterSeq: 55 });
  });
});

describe("sortConversationMessages", () => {
  const pending = (
    clientID: string,
    createdAt: string,
    id = 0,
  ) => ({
    id,
    seq: 0,
    client_message_id: clientID,
    created_at: createdAt,
  });

  it("keeps pending and failed messages in send order, not client-id order", () => {
    const sorted = sortConversationMessages([
      pending("f0", "2026-10-25T10:01:00.000Z"),
      pending("0a", "2026-10-25T10:00:00.000Z"),
    ]);
    expect(sorted.map((message) => message.client_message_id))
      .toEqual(["0a", "f0"]);
  });

  it("uses the client id only when the timestamps match", () => {
    const sorted = sortConversationMessages([
      pending("b", "2026-10-25T10:00:00.000Z"),
      pending("a", "2026-10-25T10:00:00.000Z"),
    ]);
    expect(sorted.map((message) => message.client_message_id)).toEqual(["a", "b"]);
  });

  it("orders confirmed messages by seq before any pending message", () => {
    const sorted = sortConversationMessages([
      pending("f0", "2026-10-25T09:00:00.000Z"),
      { id: 20, seq: 2, client_message_id: "", created_at: "2026-10-25T10:00:00.000Z" },
      { id: 10, seq: 1, client_message_id: "", created_at: "2026-10-25T10:00:00.000Z" },
    ]);
    expect(sorted.map((message) => message.seq)).toEqual([1, 2, 0]);
  });
});

describe("gap filling", () => {
  it("lists only the seqs absent from the local set", () => {
    expect(missingSeqs(2, 6, [3, 4, 6])).toEqual([5]);
    expect(missingSeqs(0, 3, [1, 2, 3])).toEqual([]);
    expect(missingSeqs(0, 3, [])).toEqual([1, 2, 3]);
    expect(missingSeqs(0, 0, [])).toEqual([]);
  });

  it("pulls a gap once and never retries a seq already marked missing", () => {
    const present = new Set([1, 2, 4, 5]);
    const tracker = new MissingSeqTracker();
    const first = planGapPull(0, 5, present, tracker.pending("direct:bob"));
    expect(first).toEqual({ afterSeq: 2, targetSeq: 5 });

    tracker.mark("direct:bob", [3]);
    expect(tracker.has("direct:bob", 3)).toBe(true);
    expect(planGapPull(0, 5, present, tracker.pending("direct:bob"))).toBeNull();
  });

  it("resumes at the last seq before the hole for the exclusive cursor", () => {
    // A 120-message cache with a hole at 10..12 must ask for after_seq=9; the
    // range start 0 is the endpoint's "no cursor" sentinel and would return the
    // newest page instead.
    const present = new Set(
      Array.from({ length: 120 }, (_, index) => index + 1)
        .filter((seq) => seq < 10 || seq > 12),
    );
    expect(planGapPull(0, 120, present))
      .toEqual({ afterSeq: 9, targetSeq: 120 });
    // A hole that starts at seq 1 still has to fall back to the sentinel.
    expect(planGapPull(0, 3, [2, 3])).toEqual({ afterSeq: 0, targetSeq: 3 });
  });

  it("resumes before the next gap once a skipped seq is marked", () => {
    const present = new Set([1, 2, 4, 6]);
    const missing = new Set([3]);
    expect(planGapPull(0, 6, present, missing))
      .toEqual({ afterSeq: 4, targetSeq: 6 });
  });

  it("clears a conversation's missing seqs", () => {
    const tracker = new MissingSeqTracker();
    tracker.mark("group:1", [2, 3]);
    tracker.clear("group:1");
    expect(tracker.has("group:1", 2)).toBe(false);
    expect(tracker.pending("group:1").size).toBe(0);
  });
});

describe("shouldLoadNewestPage", () => {
  it("loads the newest page for a cold list or an oversized hole", () => {
    // Cursor 0 is the server's "start at seq 1" sentinel.
    expect(shouldLoadNewestPage(0, 3500, 1500)).toBe(true);
    expect(shouldLoadNewestPage(100, 1700, 1500)).toBe(true);
    expect(shouldLoadNewestPage(5, 1706, 1500)).toBe(true);
  });

  it("keeps the forward pull for a hole within the cap", () => {
    expect(shouldLoadNewestPage(5, 9, 1500)).toBe(false);
    expect(shouldLoadNewestPage(100, 1600, 1500)).toBe(false);
  });
});

describe("local unread", () => {
  it("adds one only for a following message from someone else", () => {
    const append = { kind: "append" as const };
    expect(nextUnread(append, 4, false)).toEqual({ unread: 5, refresh: false });
    expect(nextUnread(append, 4, true)).toEqual({ unread: 4, refresh: false });
  });

  it("does not count duplicates or old seqs", () => {
    expect(nextUnread({ kind: "duplicate" }, 4, false))
      .toEqual({ unread: 4, refresh: false });
  });

  it("asks for a fresh conversation list on a gap or rollback", () => {
    expect(nextUnread(
      { kind: "pull", afterSeq: 2, targetSeq: 9 },
      4,
      false,
    )).toEqual({ unread: 4, refresh: true });
    expect(nextUnread({ kind: "resync", fromSeq: 3 }, 4, false))
      .toEqual({ unread: 4, refresh: true });
  });
});

describe("preview and receipts", () => {
  it("only moves the preview forward", () => {
    expect(shouldReplacePreview(4, 5)).toBe(true);
    expect(shouldReplacePreview(5, 5)).toBe(false);
    expect(shouldReplacePreview(6, 5)).toBe(false);
    expect(shouldReplacePreview(0, 1)).toBe(true);
    expect(shouldReplacePreview(4, 0)).toBe(false);
  });

  it("compares the peer watermark against the message seq", () => {
    expect(isReadByPeer(4, 4)).toBe(true);
    expect(isReadByPeer(4, 5)).toBe(true);
    expect(isReadByPeer(5, 4)).toBe(false);
    expect(isReadByPeer(0, 9)).toBe(false);
  });
});

describe("outbox reconciliation", () => {
  it("surfaces entries whose client id never came back confirmed", () => {
    const entries = [
      { client_message_id: "a" },
      { client_message_id: "b" },
    ];
    expect(lostOutboxEntries(entries, new Set(["a"])))
      .toEqual([{ client_message_id: "b" }]);
  });

  it("shows a stored sending row as sending only while its send is in flight", () => {
    expect(outboxDisplayStatus("sending", true)).toBe("sending");
    // A reload leaves the row behind with no request in this document.
    expect(outboxDisplayStatus("sending", false)).toBe("failed");
    expect(outboxDisplayStatus("failed", true)).toBe("failed");
    expect(outboxDisplayStatus("failed", false)).toBe("failed");
  });
});
