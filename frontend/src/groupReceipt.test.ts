import { describe, expect, it } from "vitest";
import { groupReceiptLabel, lastOwnSentMessageID } from "./groupReceipt";

type TestMessage = {
  id: number;
  status: string;
  sender: { username: string };
};

function message(
  id: number,
  username = "alice",
  status = "sent",
): TestMessage {
  return { id, status, sender: { username } };
}

describe("last own sent message", () => {
  it("returns the highest sent message of the current user", () => {
    const messages = [
      message(10),
      message(11, "bob"),
      message(12),
      message(13, "bob"),
    ];
    expect(lastOwnSentMessageID(messages, "alice")).toBe(12);
  });

  it("ignores sending, failed and unsaved messages", () => {
    const messages = [
      message(10),
      message(0, "alice", "sending"),
      message(11, "alice", "sending"),
      message(12, "alice", "failed"),
    ];
    expect(lastOwnSentMessageID(messages, "alice")).toBe(10);
  });

  it("matches usernames case-insensitively", () => {
    expect(lastOwnSentMessageID([message(7, "Alice")], "alice")).toBe(7);
  });

  it("returns zero when nothing qualifies", () => {
    expect(lastOwnSentMessageID([], "alice")).toBe(0);
    expect(lastOwnSentMessageID([message(3, "bob")], "alice")).toBe(0);
  });
});

describe("group receipt label", () => {
  it("shows the partial count", () => {
    expect(groupReceiptLabel({
      message_id: 9,
      read_count: 3,
      member_count: 5,
      all_read: false,
    })).toBe("已读 3/5");
    expect(groupReceiptLabel({
      message_id: 9,
      read_count: 0,
      member_count: 2,
      all_read: false,
    })).toBe("已读 0/2");
  });

  it("shows the all-read label", () => {
    expect(groupReceiptLabel({
      message_id: 9,
      read_count: 2,
      member_count: 2,
      all_read: true,
    })).toBe("全部已读");
  });
});
