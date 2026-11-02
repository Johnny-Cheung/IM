import { describe, expect, it } from "vitest";
import type { PublicUser } from "../api";
import {
  conversationKey,
  directKey,
  groupKey,
} from "./keys";

const alice: PublicUser = { username: "Alice", nickname: "Alice" };
const bob: PublicUser = { username: "Bob", nickname: "Bob" };

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
      last_seq: 0,
      unread_count: 0,
      peer_last_read_seq: 0,
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
      last_seq: 0,
      unread_count: 0,
    })).toBe("group:9");
  });
});
