import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import type {
  ChatMessage,
  Conversation,
  DirectConversation,
  GroupConversation,
  GroupMessage,
  PublicUser,
} from "../api";
import { conversationKey } from "./keys";
import {
  cacheSupported,
  clearUser,
  enforceUserLimits,
  patchSyncState,
  readConversations,
  readFriends,
  readHints,
  readMessages,
  readSyncState,
  removeConversation,
  removeGroupCursor,
  resetCacheForTests,
  setActiveUser,
  setGroupCursor,
  writeConversations,
  writeFriends,
  writeMessages,
  writeSyncState,
} from "./store";
import { emptySyncState } from "./types";
import type { SyncState } from "./types";

const alice: PublicUser = { username: "alice", nickname: "Alice" };
const bob: PublicUser = { username: "bob", nickname: "Bob" };

function directMessage(
  id: number,
  overrides: Partial<ChatMessage> = {},
): ChatMessage {
  return {
    id,
    sender: alice,
    recipient: bob,
    body: `direct-${id}`,
    client_message_id: `direct-client-${id}`,
    created_at: "2026-10-26T00:00:00Z",
    ...overrides,
  };
}

function groupMessage(
  groupID: number,
  id: number,
  overrides: Partial<GroupMessage> = {},
): GroupMessage {
  return {
    id,
    group_id: groupID,
    sender: alice,
    body: `group-${groupID}-${id}`,
    client_message_id: `group-client-${groupID}-${id}`,
    created_at: "2026-10-26T00:00:00Z",
    ...overrides,
  };
}

function directConversation(
  username: string,
  lastMessageID: number | null,
): DirectConversation {
  const peer: PublicUser = { username, nickname: username };
  return {
    type: "direct",
    user: peer,
    is_friend: true,
    last_message:
      lastMessageID === null
        ? null
        : {
            id: lastMessageID,
            sender: alice,
            recipient: peer,
            body: `direct-${lastMessageID}`,
            client_message_id: `direct-client-${lastMessageID}`,
            created_at: "2026-10-26T00:00:00Z",
          },
    unread_count: 0,
    peer_last_read_message_id: 0,
  };
}

function groupConversation(
  groupID: number,
  lastMessageID: number | null,
): GroupConversation {
  return {
    type: "group",
    group: {
      id: groupID,
      name: `Group ${groupID}`,
      owner: alice,
      member_count: 2,
      created_at: "2026-10-26T00:00:00Z",
    },
    is_owner: false,
    last_message:
      lastMessageID === null
        ? null
        : {
            id: lastMessageID,
            group_id: groupID,
            sender: alice,
            body: `group-${lastMessageID}`,
            client_message_id: `group-client-${lastMessageID}`,
            created_at: "2026-10-26T00:00:00Z",
          },
    unread_count: 0,
  };
}

function keysOf(conversations: Conversation[]): string[] {
  return conversations.map(conversationKey).sort();
}

function idsOf(messages: Array<{ id: number }>): number[] {
  return messages.map((message) => message.id);
}

beforeEach(async () => {
  await resetCacheForTests();
  globalThis.indexedDB = new IDBFactory();
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("hints", () => {
  it("returns empty hints before anything is stored", async () => {
    expect(cacheSupported()).toBe(true);
    expect(await readHints()).toEqual({ user: null, sync: null });
  });

  it("reads the active user and that user's sync state", async () => {
    const state: SyncState = {
      afterID: 42,
      directComplete: false,
      groups: { 7: 99 },
      savedAt: "2026-10-26T00:00:00.000Z",
    };
    await setActiveUser(alice);
    expect(await readHints()).toEqual({ user: alice, sync: null });

    await writeSyncState("alice", state);

    expect(await readHints()).toEqual({ user: alice, sync: state });
  });
});

describe("conversations", () => {
  it("roundtrips conversations per user", async () => {
    const direct = directConversation("bob", 10);
    const group = groupConversation(5, 20);

    await writeConversations("alice", [direct, group]);

    expect(keysOf(await readConversations("alice"))).toEqual([
      "direct:bob",
      "group:5",
    ]);
    expect(await readConversations("bob")).toEqual([]);
  });

  it("removes cached messages and group cursors of dropped conversations", async () => {
    const direct = directConversation("bob", 10);
    const group = groupConversation(5, 20);

    await writeConversations("alice", [direct, group]);
    await writeMessages("alice", "group:5", [groupMessage(5, 100)]);
    await writeMessages("alice", "direct:bob", [directMessage(100)]);
    await setGroupCursor("alice", 5, 100);

    await writeConversations("alice", [direct]);

    expect(keysOf(await readConversations("alice"))).toEqual(["direct:bob"]);
    expect(await readMessages("alice", "group:5")).toEqual([]);
    expect(await readMessages("alice", "direct:bob")).toHaveLength(1);
    expect((await readSyncState("alice")).groups).toEqual({});
  });

  it("removes a single conversation, its messages, and its group cursor", async () => {
    await writeConversations("alice", [groupConversation(5, 20)]);
    await writeMessages("alice", "group:5", [groupMessage(5, 100)]);
    await setGroupCursor("alice", 5, 100);

    await removeConversation("alice", "group:5");

    expect(await readConversations("alice")).toEqual([]);
    expect(await readMessages("alice", "group:5")).toEqual([]);
    expect((await readSyncState("alice")).groups).toEqual({});
  });
});

describe("friends", () => {
  it("roundtrips friends per user", async () => {
    await writeFriends("alice", [bob]);
    await writeFriends("bob", [alice]);

    expect(await readFriends("alice")).toEqual([bob]);
    expect(await readFriends("bob")).toEqual([alice]);
    expect(await readFriends("carol")).toEqual([]);
  });
});

describe("messages", () => {
  it("dedupes by id and client id and is idempotent", async () => {
    const conversation = "direct:bob";
    await writeMessages("alice", conversation, [
      directMessage(1),
      directMessage(2),
    ]);

    await writeMessages("alice", conversation, [
      directMessage(2, { body: "updated", client_message_id: "changed" }),
      directMessage(3),
    ]);

    let cached = await readMessages("alice", conversation);
    expect(idsOf(cached)).toEqual([1, 2, 3]);
    expect(cached[1].body).toBe("updated");

    await writeMessages("alice", conversation, [
      directMessage(2, { body: "updated", client_message_id: "changed" }),
      directMessage(3),
    ]);

    cached = await readMessages("alice", conversation);
    expect(idsOf(cached)).toEqual([1, 2, 3]);
    expect(cached[1].body).toBe("updated");
  });

  it("keeps two confirmed messages that share a client id", async () => {
    const conversation = "direct:bob";
    await writeMessages("alice", conversation, [
      directMessage(1, { client_message_id: "shared" }),
    ]);

    await writeMessages("alice", conversation, [
      directMessage(4, { client_message_id: "shared", body: "server copy" }),
    ]);

    const cached = await readMessages("alice", conversation);
    expect(idsOf(cached)).toEqual([1, 4]);
  });

  it("returns ascending by id and the newest messages for a limit", async () => {
    const conversation = "direct:bob";
    await writeMessages(
      "alice",
      conversation,
      [5, 1, 3, 2, 4].map((id) => directMessage(id)),
    );

    expect(idsOf(await readMessages("alice", conversation))).toEqual([
      1, 2, 3, 4, 5,
    ]);
    expect(
      idsOf(await readMessages("alice", conversation, 2)),
    ).toEqual([4, 5]);
    expect(await readMessages("alice", conversation, 0)).toEqual([]);
  });

  it("keeps only the newest 200 messages per conversation", async () => {
    const conversation = "direct:bob";
    const messages = Array.from({ length: 250 }, (_, index) =>
      directMessage(index + 1),
    );

    await writeMessages("alice", conversation, messages.slice(0, 120));
    await writeMessages("alice", conversation, messages.slice(120));

    const cached = await readMessages("alice", conversation);
    expect(idsOf(cached)).toEqual(
      Array.from({ length: 200 }, (_, index) => index + 51),
    );
    expect(
      idsOf(await readMessages("alice", conversation, 3)),
    ).toEqual([248, 249, 250]);
  });
});

describe("sync state", () => {
  it("returns an empty state when absent and stores explicit state", async () => {
    expect(await readSyncState("alice")).toEqual(emptySyncState());

    const state: SyncState = {
      afterID: 10,
      directComplete: true,
      groups: { 3: 30 },
      savedAt: "2026-10-26T00:00:00.000Z",
    };
    await writeSyncState("alice", state);

    expect(await readSyncState("alice")).toEqual(state);
  });

  it("patches shallowly, keeps groups, and refreshes savedAt", async () => {
    await writeSyncState("alice", {
      afterID: 10,
      directComplete: true,
      groups: { 3: 30 },
      savedAt: "2026-10-26T00:00:00.000Z",
    });

    await patchSyncState("alice", { afterID: 15 });

    const patched = await readSyncState("alice");
    expect(patched.afterID).toBe(15);
    expect(patched.directComplete).toBe(true);
    expect(patched.groups).toEqual({ 3: 30 });
    expect(patched.savedAt).not.toBe("2026-10-26T00:00:00.000Z");
    expect(Number.isNaN(Date.parse(patched.savedAt))).toBe(false);
  });

  it("sets and removes group cursors", async () => {
    await setGroupCursor("alice", 3, 33);
    await setGroupCursor("alice", 4, 44);

    expect((await readSyncState("alice")).groups).toEqual({ 3: 33, 4: 44 });

    await removeGroupCursor("alice", 3);

    expect((await readSyncState("alice")).groups).toEqual({ 4: 44 });
    expect((await readSyncState("bob")).groups).toEqual({});
  });

  it("never moves cursors backwards", async () => {
    await patchSyncState("alice", { afterID: 50 });
    await patchSyncState("alice", { afterID: 10 });
    await setGroupCursor("alice", 7, 30);
    await setGroupCursor("alice", 7, 20);
    await setGroupCursor("alice", 7, 0);
    await setGroupCursor("alice", 0, 5);

    const state = await readSyncState("alice");
    expect(state.afterID).toBe(50);
    expect(state.groups).toEqual({ 7: 30 });
  });
});

describe("clearUser", () => {
  it("removes only the given user's cache", async () => {
    await setActiveUser(alice);
    await writeConversations("alice", [directConversation("bob", 10)]);
    await writeMessages("alice", "direct:bob", [directMessage(10)]);
    await writeFriends("alice", [bob]);
    await writeSyncState("alice", {
      afterID: 10,
      directComplete: true,
      groups: { 1: 10 },
      savedAt: "2026-10-26T00:00:00.000Z",
    });

    await writeConversations("bob", [directConversation("carol", 20)]);
    await writeMessages("bob", "direct:carol", [
      directMessage(20, { sender: bob, recipient: { username: "carol", nickname: "Carol" } }),
    ]);
    await writeFriends("bob", [alice]);

    await clearUser("alice");

    expect(await readConversations("alice")).toEqual([]);
    expect(await readMessages("alice", "direct:bob")).toEqual([]);
    expect(await readFriends("alice")).toEqual([]);
    expect(await readSyncState("alice")).toEqual(emptySyncState());
    expect(await readHints()).toEqual({ user: null, sync: null });

    expect(keysOf(await readConversations("bob"))).toEqual(["direct:carol"]);
    expect(await readMessages("bob", "direct:carol")).toHaveLength(1);
    expect(await readFriends("bob")).toEqual([alice]);
  });

  it("keeps the active user hint when it belongs to another user", async () => {
    await setActiveUser(bob);
    await writeConversations("alice", [directConversation("bob", 10)]);

    await clearUser("alice");

    expect(await readHints()).toEqual({ user: bob, sync: null });
  });
});

describe("enforceUserLimits", () => {
  it("keeps messages whose conversation record is not cached yet", async () => {
    await writeMessages("alice", "group:9", [groupMessage(9, 90)]);

    await enforceUserLimits("alice", {
      maxMessages: 10_000,
      maxConversations: 100,
    });

    expect(idsOf(await readMessages("alice", "group:9"))).toEqual([90]);
  });

  it("evicts whole conversations and their cursors by newest message id", async () => {
    await writeConversations("alice", [
      groupConversation(1, 10),
      groupConversation(2, 20),
      groupConversation(3, 30),
    ]);
    await writeMessages("alice", "group:1", [
      groupMessage(1, 10),
      groupMessage(1, 11),
    ]);
    await writeMessages("alice", "group:2", [
      groupMessage(2, 20),
      groupMessage(2, 21),
    ]);
    await writeMessages("alice", "group:3", [
      groupMessage(3, 30),
      groupMessage(3, 31),
    ]);
    await setGroupCursor("alice", 1, 11);
    await setGroupCursor("alice", 2, 21);
    await setGroupCursor("alice", 3, 31);

    await enforceUserLimits("alice", {
      maxMessages: 2,
      maxConversations: 100,
    });

    expect(await readMessages("alice", "group:1")).toEqual([]);
    expect(await readMessages("alice", "group:2")).toEqual([]);
    expect(idsOf(await readMessages("alice", "group:3"))).toEqual([30, 31]);
    expect((await readSyncState("alice")).groups).toEqual({ 3: 31 });
    expect(keysOf(await readConversations("alice"))).toEqual([
      "group:1",
      "group:2",
      "group:3",
    ]);
  });

  it("also enforces the conversation limit", async () => {
    await writeConversations("alice", [
      groupConversation(1, 10),
      groupConversation(2, 20),
      groupConversation(3, 30),
    ]);
    await writeMessages("alice", "group:1", [groupMessage(1, 10)]);
    await writeMessages("alice", "group:2", [groupMessage(2, 20)]);
    await writeMessages("alice", "group:3", [groupMessage(3, 30)]);
    await setGroupCursor("alice", 3, 30);

    await enforceUserLimits("alice", {
      maxMessages: 10_000,
      maxConversations: 1,
    });

    expect(await readMessages("alice", "group:1")).toEqual([]);
    expect(await readMessages("alice", "group:2")).toEqual([]);
    expect(await readMessages("alice", "group:3")).toHaveLength(1);
    expect((await readSyncState("alice")).groups).toEqual({ 3: 30 });
  });
});

describe("degraded mode", () => {
  it("returns empty values and silently ignores writes without IndexedDB", async () => {
    Object.defineProperty(globalThis, "indexedDB", {
      configurable: true,
      writable: true,
      value: undefined,
    });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    expect(cacheSupported()).toBe(false);
    await expect(readHints()).resolves.toEqual({ user: null, sync: null });
    await expect(readConversations("alice")).resolves.toEqual([]);
    await expect(readFriends("alice")).resolves.toEqual([]);
    await expect(readMessages("alice", "direct:bob")).resolves.toEqual([]);
    await expect(readSyncState("alice")).resolves.toEqual(emptySyncState());

    await expect(setActiveUser(alice)).resolves.toBeUndefined();
    await expect(writeConversations("alice", [])).resolves.toBeUndefined();
    await expect(
      writeMessages("alice", "direct:bob", [directMessage(1)]),
    ).resolves.toBeUndefined();
    await expect(writeFriends("alice", [bob])).resolves.toBeUndefined();
    await expect(
      patchSyncState("alice", { afterID: 1 }),
    ).resolves.toBeUndefined();
    await expect(setGroupCursor("alice", 1, 1)).resolves.toBeUndefined();
    await expect(removeGroupCursor("alice", 1)).resolves.toBeUndefined();
    await expect(
      removeConversation("alice", "direct:bob"),
    ).resolves.toBeUndefined();
    await expect(clearUser("alice")).resolves.toBeUndefined();
    await expect(enforceUserLimits("alice")).resolves.toBeUndefined();

    expect(warn).toHaveBeenCalledTimes(1);
  });

  it("disables caching after an open failure and can be reset", async () => {
    await resetCacheForTests();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const open = vi
      .spyOn(IDBFactory.prototype, "open")
      .mockImplementation(() => {
        throw new Error("blocked");
      });

    await expect(readConversations("alice")).resolves.toEqual([]);
    await expect(writeFriends("alice", [bob])).resolves.toBeUndefined();
    expect(warn).toHaveBeenCalledTimes(1);

    open.mockRestore();
    await resetCacheForTests();
    await writeFriends("alice", [bob]);
    expect(await readFriends("alice")).toEqual([bob]);
  });
});

describe("storage guarantees", () => {
  it("merges an older page without dropping newer cached messages", async () => {
    await writeMessages("alice", "direct:bob", [
      directMessage(10),
      directMessage(11),
    ]);

    await writeMessages("alice", "direct:bob", [directMessage(9)]);

    expect(idsOf(await readMessages("alice", "direct:bob"))).toEqual([9, 10, 11]);
  });

  it("persists only the newest 200 messages per conversation", async () => {
    const messages = Array.from({ length: 250 }, (_, index) =>
      directMessage(index + 1),
    );

    await writeMessages("alice", "direct:bob", messages);

    const all = await readMessages("alice", "direct:bob", 10_000);
    expect(all).toHaveLength(200);
    expect(idsOf(all)).toEqual(
      Array.from({ length: 200 }, (_, index) => index + 51),
    );
  });

  it("evicts the conversation with the oldest message id", async () => {
    await writeConversations("alice", [
      groupConversation(9, 10),
      groupConversation(1, 100),
    ]);
    await writeMessages("alice", "group:9", [groupMessage(9, 10)]);
    await writeMessages("alice", "group:1", [groupMessage(1, 100)]);
    await setGroupCursor("alice", 9, 10);
    await setGroupCursor("alice", 1, 100);

    await enforceUserLimits("alice", {
      maxConversations: 1,
      maxMessages: 10_000,
    });

    expect(await readMessages("alice", "group:9")).toEqual([]);
    expect(idsOf(await readMessages("alice", "group:1"))).toEqual([100]);
    expect((await readSyncState("alice")).groups).toEqual({ 1: 100 });
  });

  it("drops group cursors when their messages are evicted", async () => {
    await writeMessages("alice", "group:9", [groupMessage(9, 90)]);
    await setGroupCursor("alice", 9, 90);

    await enforceUserLimits("alice", {
      maxMessages: 10_000,
      maxConversations: 0,
    });

    expect(await readMessages("alice", "group:9")).toEqual([]);
    expect((await readSyncState("alice")).groups).toEqual({});
  });

  it("isolates message caches and limits per user", async () => {
    await writeMessages("alice", "direct:carol", [
      directMessage(10, { body: "alice-body" }),
    ]);
    await writeMessages("bob", "direct:carol", [
      directMessage(10, {
        sender: bob,
        recipient: alice,
        body: "bob-body",
        client_message_id: "bob-client-10",
      }),
    ]);
    await setGroupCursor("bob", 5, 50);

    await enforceUserLimits("alice", { maxMessages: 0 });

    expect(await readMessages("alice", "direct:carol")).toEqual([]);
    expect((await readMessages("bob", "direct:carol")).map((item) => item.body))
      .toEqual(["bob-body"]);
    expect((await readSyncState("bob")).groups).toEqual({ 5: 50 });
  });

  it("ignores unconfirmed messages without a server id", async () => {
    await writeMessages("alice", "direct:bob", [directMessage(5)]);

    await writeMessages("alice", "direct:bob", [
      directMessage(0, {
        client_message_id: "direct-client-5",
        body: "optimistic",
      }),
    ]);

    const cached = await readMessages("alice", "direct:bob");
    expect(cached).toHaveLength(1);
    expect(cached[0].body).toBe("direct-5");
  });

  it("removes a direct conversation without touching its neighbour", async () => {
    await writeConversations("alice", [
      directConversation("bob", 10),
      directConversation("carol", 11),
    ]);
    await writeMessages("alice", "direct:bob", [directMessage(10)]);
    await writeMessages("alice", "direct:carol", [directMessage(11)]);

    await removeConversation("alice", "direct:bob");

    expect(keysOf(await readConversations("alice"))).toEqual(["direct:carol"]);
    expect(await readMessages("alice", "direct:bob")).toEqual([]);
    expect(idsOf(await readMessages("alice", "direct:carol"))).toEqual([11]);
  });

  it("keeps groups on a partial patch and merges them when supplied", async () => {
    await patchSyncState("alice", {
      afterID: 10,
      directComplete: true,
      groups: { 1: 5, 2: 6 },
    });

    await patchSyncState("alice", { directComplete: false });
    let state = await readSyncState("alice");
    expect(state.afterID).toBe(10);
    expect(state.directComplete).toBe(false);
    expect(state.groups).toEqual({ 1: 5, 2: 6 });

    await patchSyncState("alice", { groups: { 3: 7 } });
    state = await readSyncState("alice");
    expect(state.groups).toEqual({ 1: 5, 2: 6, 3: 7 });
  });
});
