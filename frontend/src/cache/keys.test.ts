import { describe, expect, it } from "vitest";
import type { ChatMessage, GroupMessage, PublicUser } from "../api";
import {
  conversationKey,
  directKey,
  groupIDFromKey,
  groupKey,
  messageConversationKey,
} from "./keys";

const alice: PublicUser = { username: "Alice", nickname: "Alice" };
const bob: PublicUser = { username: "Bob", nickname: "Bob" };

function directMessage(): ChatMessage {
  return {
    id: 1,
    sender: alice,
    recipient: bob,
    body: "hello",
    client_message_id: "client-00000001",
    created_at: "2026-10-26T00:00:00Z",
  };
}

function groupMessage(): GroupMessage {
  return {
    id: 2,
    group_id: 9,
    sender: bob,
    body: "hello group",
    client_message_id: "client-00000002",
    created_at: "2026-10-26T00:00:00Z",
  };
}

describe("conversation keys", () => {
  it("lowercases direct usernames so one peer maps to one cache key", () => {
    expect(directKey("BoB")).toBe("direct:bob");
    expect(directKey("bob")).toBe("direct:bob");
  });

  it("builds group keys from the group id", () => {
    expect(groupKey(9)).toBe("group:9");
  });

  it("derives a key from either conversation type", () => {
    expect(conversationKey({
      type: "direct",
      user: bob,
      is_friend: true,
      last_message: null,
      unread_count: 0,
      peer_last_read_message_id: 0,
    })).toBe("direct:bob");
    expect(conversationKey({
      type: "group",
      group: {
        id: 9,
        name: "Team",
        owner: alice,
        member_count: 2,
        created_at: "2026-10-26T00:00:00Z",
      },
      is_owner: false,
      last_message: null,
      unread_count: 0,
    })).toBe("group:9");
  });

  it("parses only positive integer group keys", () => {
    expect(groupIDFromKey("group:9")).toBe(9);
    expect(groupIDFromKey("group:0")).toBeNull();
    expect(groupIDFromKey("group:")).toBeNull();
    expect(groupIDFromKey("group:abc")).toBeNull();
    expect(groupIDFromKey("group:1.5")).toBeNull();
    expect(groupIDFromKey("group:-1")).toBeNull();
    expect(groupIDFromKey("direct:bob")).toBeNull();
  });

  it("resolves the peer side of a direct message case-insensitively", () => {
    expect(messageConversationKey(directMessage(), "alice")).toBe("direct:bob");
    expect(messageConversationKey(directMessage(), "BOB")).toBe("direct:alice");
    expect(messageConversationKey(directMessage(), "carol")).toBe("");
  });

  it("uses the group id for group messages regardless of the current user", () => {
    expect(messageConversationKey(groupMessage(), "alice")).toBe("group:9");
    expect(messageConversationKey(groupMessage(), "bob")).toBe("group:9");
  });
});
