import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { api } from "./api";
import type {
  BootstrapGroupDelta,
  BootstrapResponse,
  ChatMessage,
  Conversation,
  GroupMessage,
  PublicUser,
} from "./api";
import {
  readConversations,
  readFriends,
  readMessages,
  readSyncState,
  resetCacheForTests,
} from "./cache";
import { applyBootstrapToCache } from "./workspace";

const alice: PublicUser = { username: "alice", nickname: "Alice" };
const bob: PublicUser = { username: "bob", nickname: "Bob" };
const carol: PublicUser = { username: "carol", nickname: "Carol" };

function directMessage(
  id: number,
  sender: PublicUser,
  recipient: PublicUser,
): ChatMessage {
  return {
    id,
    sender,
    recipient,
    body: `message-${id}`,
    client_message_id: `client-${id}`,
    created_at: "2026-10-26T00:00:00Z",
  };
}

function directConversation(username: string, lastID: number): Conversation {
  const peer: PublicUser = { username, nickname: username };
  return {
    type: "direct",
    user: peer,
    is_friend: true,
    last_message: {
      id: lastID,
      sender: peer,
      recipient: alice,
      body: `last-${lastID}`,
      client_message_id: `last-client-${lastID}`,
      created_at: "2026-10-26T00:00:00Z",
    },
    unread_count: 0,
    peer_last_read_message_id: 0,
  };
}

function groupConversation(id: number, lastID: number): Conversation {
  return {
    type: "group",
    group: {
      id,
      name: `Group ${id}`,
      owner: alice,
      member_count: 2,
      created_at: "2026-10-26T00:00:00Z",
    },
    is_owner: false,
    last_message: {
      id: lastID,
      group_id: id,
      sender: bob,
      body: `group-last-${lastID}`,
      client_message_id: `group-last-client-${lastID}`,
      created_at: "2026-10-26T00:00:00Z",
    },
    unread_count: 0,
  };
}

function groupMessage(
  id: number,
  groupID: number,
  sender: PublicUser = bob,
): GroupMessage {
  return {
    id,
    group_id: groupID,
    sender,
    body: `group-${id}`,
    client_message_id: `group-client-${id}`,
    created_at: "2026-10-26T00:00:00Z",
  };
}

function bootstrapPayload(overrides: {
  messages?: ChatMessage[];
  cursor?: number;
  hasMore?: boolean;
  complete?: boolean;
  partial?: string[];
  conversations?: Conversation[];
  groups?: BootstrapGroupDelta[];
} = {}): BootstrapResponse {
  return {
    user: alice,
    conversations: overrides.conversations ?? [
      directConversation("bob", 5),
      directConversation("carol", 6),
    ],
    friends: [bob],
    direct: {
      messages: overrides.messages ?? [],
      has_more: overrides.hasMore ?? false,
      complete: overrides.complete ?? true,
      cursor: overrides.cursor ?? 0,
    },
    groups: overrides.groups ?? [],
    partial: overrides.partial ?? [],
  };
}

beforeEach(async () => {
  await resetCacheForTests();
  globalThis.indexedDB = new IDBFactory();
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("applyBootstrapToCache", () => {
  it("files direct messages under their conversation and stores the cursor", async () => {
    const payload = bootstrapPayload({
      messages: [
        directMessage(5, bob, alice),
        directMessage(6, alice, carol),
      ],
      cursor: 6,
    });

    await applyBootstrapToCache(payload);

    expect((await readMessages("alice", "direct:bob")).map((item) => item.id))
      .toEqual([5]);
    expect((await readMessages("alice", "direct:carol")).map((item) => item.id))
      .toEqual([6]);
    expect((await readConversations("alice")).map((item) =>
      item.type === "direct" ? item.user.username : "group")).toEqual(["bob", "carol"]);
    expect(await readFriends("alice")).toEqual([bob]);
    const state = await readSyncState("alice");
    expect(state.afterID).toBe(6);
    expect(state.directComplete).toBe(true);
  });

  it("skips messages that do not involve the current user", async () => {
    const payload = bootstrapPayload({
      messages: [directMessage(7, bob, carol)],
      cursor: 7,
    });

    await applyBootstrapToCache(payload);

    expect(await readMessages("alice", "direct:bob")).toEqual([]);
    expect(await readMessages("alice", "direct:carol")).toEqual([]);
  });

  it("drains a truncated feed in the background", async () => {
    const sync = vi.spyOn(api, "syncMessages").mockResolvedValue({
      messages: [directMessage(6, bob, alice)],
      has_more: false,
      complete: true,
      cursor: 6,
    });
    const payload = bootstrapPayload({
      messages: [directMessage(5, bob, alice)],
      cursor: 5,
      hasMore: true,
    });

    await applyBootstrapToCache(payload);

    expect(sync).toHaveBeenCalledWith(5, 200);
    expect((await readMessages("alice", "direct:bob")).map((item) => item.id))
      .toEqual([5, 6]);
    expect((await readSyncState("alice")).afterID).toBe(6);
  });

  it("marks the direct cache unreliable when the feed is incomplete", async () => {
    const payload = bootstrapPayload({
      messages: [directMessage(8, bob, alice)],
      cursor: 8,
      complete: false,
    });

    await applyBootstrapToCache(payload);

    expect((await readSyncState("alice")).directComplete).toBe(false);
  });

  it("does not overwrite conversations when that section is partial", async () => {
    await applyBootstrapToCache(bootstrapPayload({
      conversations: [directConversation("bob", 5)],
    }));
    vi.spyOn(api, "syncMessages");

    await applyBootstrapToCache(bootstrapPayload({
      messages: [directMessage(9, bob, alice)],
      cursor: 9,
      partial: ["conversations"],
    }));

    expect((await readConversations("alice")).map((item) =>
      item.type === "direct" ? item.user.username : "group")).toEqual(["bob"]);
    expect((await readMessages("alice", "direct:bob")).map((item) => item.id))
      .toEqual([9]);
  });

  it("caches complete group deltas and their cursors", async () => {
    const payload = bootstrapPayload({
      conversations: [groupConversation(9, 42)],
      groups: [{
        group_id: 9,
        messages: [groupMessage(41, 9), groupMessage(42, 9)],
        has_more: false,
        complete: true,
        cursor: 42,
      }],
    });

    await applyBootstrapToCache(payload);

    expect((await readMessages("alice", "group:9")).map((item) => item.id))
      .toEqual([41, 42]);
    expect((await readSyncState("alice")).groups).toEqual({ 9: 42 });
  });

  it("drops a cached group when its delta is incomplete", async () => {
    await applyBootstrapToCache(bootstrapPayload({
      conversations: [groupConversation(9, 40)],
      groups: [{
        group_id: 9,
        messages: [groupMessage(40, 9)],
        has_more: false,
        complete: true,
        cursor: 40,
      }],
    }));

    await applyBootstrapToCache(bootstrapPayload({
      conversations: [groupConversation(9, 60)],
      groups: [{
        group_id: 9,
        messages: [],
        has_more: false,
        complete: false,
        cursor: 40,
      }],
    }));

    expect(await readMessages("alice", "group:9")).toEqual([]);
    expect((await readSyncState("alice")).groups).toEqual({});
  });

  it("drains a truncated group delta", async () => {
    const after = vi.spyOn(api, "listGroupMessagesAfter").mockResolvedValue({
      messages: [groupMessage(42, 9)],
      has_more: false,
    });
    const payload = bootstrapPayload({
      conversations: [groupConversation(9, 42)],
      groups: [{
        group_id: 9,
        messages: [groupMessage(41, 9)],
        has_more: true,
        complete: true,
        cursor: 41,
      }],
    });

    await applyBootstrapToCache(payload);

    expect(after).toHaveBeenCalledWith(9, 41, 50);
    expect((await readMessages("alice", "group:9")).map((item) => item.id))
      .toEqual([41, 42]);
    expect((await readSyncState("alice")).groups).toEqual({ 9: 42 });
  });
});
