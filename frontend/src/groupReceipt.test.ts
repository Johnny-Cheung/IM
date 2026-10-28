import { describe, expect, it } from "vitest";
import { groupReceiptLabel, lastOwnSentSeq } from "./groupReceipt";

type TestMessage = {
  id: number;
  seq: number;
  status: string;
  sender: { username: string };
};

function message(
  seq: number,
  username = "alice",
  status = "sent",
): TestMessage {
  return { id: seq + 100, seq, status, sender: { username } };
}

describe("last own sent message", () => {
  it("returns the seq of the newest sent message of the current user", () => {
    const messages = [
      message(10),
      message(11, "bob"),
      message(12),
      message(13, "bob"),
    ];
    expect(lastOwnSentSeq(messages, "alice")).toBe(12);
  });

  it("ignores sending, failed and unnumbered messages", () => {
    const messages = [
      message(10),
      { ...message(0, "alice", "sending"), id: 0 },
      message(11, "alice", "sending"),
      message(12, "alice", "failed"),
    ];
    expect(lastOwnSentSeq(messages, "alice")).toBe(10);
  });

  it("matches usernames case-insensitively", () => {
    expect(lastOwnSentSeq([message(7, "Alice")], "alice")).toBe(7);
  });

  it("returns zero when nothing qualifies", () => {
    expect(lastOwnSentSeq([], "alice")).toBe(0);
    expect(lastOwnSentSeq([message(3, "bob")], "alice")).toBe(0);
  });
});

describe("group receipt label", () => {
  it("shows the partial count", () => {
    expect(groupReceiptLabel({
      seq: 9,
      message_id: 109,
      read_count: 3,
      member_count: 5,
      all_read: false,
    })).toBe("已读 3/5");
    expect(groupReceiptLabel({
      seq: 9,
      message_id: 109,
      read_count: 0,
      member_count: 2,
      all_read: false,
    })).toBe("已读 0/2");
  });

  it("shows the all-read label", () => {
    expect(groupReceiptLabel({
      seq: 9,
      message_id: 109,
      read_count: 2,
      member_count: 2,
      all_read: true,
    })).toBe("全部已读");
  });
});
