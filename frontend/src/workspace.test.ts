import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import type {
  BootstrapResponse,
  ChatMessage,
  Conversation,
  PublicUser,
} from "./api";
import {
  readConversations,
  readFriends,
  readMessages,
  readSyncState,
  resetCacheForTests,
  writeConversations,
  writeMessages,
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
    seq: id,
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
      seq: lastID,
      sender: peer,
      recipient: alice,
      body: `last-${lastID}`,
      client_message_id: `last-client-${lastID}`,
      created_at: "2026-10-26T00:00:00Z",
    },
    last_seq: lastID,
    unread_count: 0,
    peer_last_read_seq: 0,
  };
}

function bootstrapPayload(overrides: {
  partial?: string[];
  conversations?: Conversation[];
  friends?: PublicUser[];
} = {}): BootstrapResponse {
  return {
    user: alice,
    conversations: overrides.conversations ?? [
      directConversation("bob", 5),
      directConversation("carol", 6),
    ],
    friends: overrides.friends ?? [bob],
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
  it("writes the conversation list and friends without touching messages", async () => {
    await writeMessages("alice", "direct:bob", [directMessage(5, bob, alice)]);

    await applyBootstrapToCache(bootstrapPayload());

    expect((await readConversations("alice")).map((item) =>
      item.type === "direct" ? item.user.username : "group")).toEqual(["bob", "carol"]);
    expect(await readFriends("alice")).toEqual([bob]);
    expect((await readMessages("alice", "direct:bob")).map((item) => item.id))
      .toEqual([5]);
    expect((await readSyncState("alice")).localSeq).toEqual({ "direct:bob": 5 });
  });

  it("does not overwrite conversations when that section is partial", async () => {
    await writeConversations("alice", [directConversation("bob", 5)]);

    await applyBootstrapToCache(bootstrapPayload({
      conversations: [directConversation("carol", 6)],
      partial: ["conversations"],
    }));

    expect((await readConversations("alice")).map((item) =>
      item.type === "direct" ? item.user.username : "group")).toEqual(["bob"]);
  });

  it("does not overwrite friends when that section is partial", async () => {
    await applyBootstrapToCache(bootstrapPayload({ friends: [bob] }));

    await applyBootstrapToCache(bootstrapPayload({
      friends: [carol],
      partial: ["friends"],
    }));

    expect(await readFriends("alice")).toEqual([bob]);
  });

  it("removes conversations and messages missing from a complete list", async () => {
    await writeConversations("alice", [
      directConversation("bob", 5),
      directConversation("carol", 6),
    ]);
    await writeMessages("alice", "direct:bob", [directMessage(5, bob, alice)]);
    await writeMessages("alice", "direct:carol", [directMessage(6, carol, alice)]);

    await applyBootstrapToCache(bootstrapPayload({
      conversations: [directConversation("bob", 5)],
    }));

    expect((await readConversations("alice")).map((item) =>
      item.type === "direct" ? item.user.username : "group")).toEqual(["bob"]);
    expect(await readMessages("alice", "direct:carol")).toEqual([]);
    expect((await readMessages("alice", "direct:bob")).map((item) => item.id))
      .toEqual([5]);
  });
});
