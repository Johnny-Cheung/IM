import { describe, expect, it } from "vitest";
import type {
  ChatMessage,
  Conversation,
  DirectConversation,
  GroupConversation,
  PublicUser,
} from "../api";
import {
  mergeMessages,
  planGroupSync,
  sortMessagesByID,
  trimMessageIDs,
} from "./merge";

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

function groupConversation(
  id: number,
  lastMessageID: number | null,
): GroupConversation {
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
    last_message:
      lastMessageID === null
        ? null
        : {
            id: lastMessageID,
            group_id: id,
            sender: alice,
            body: `group-${lastMessageID}`,
            client_message_id: `group-client-${lastMessageID}`,
            created_at: "2026-10-26T00:00:00Z",
          },
    unread_count: 0,
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

describe("mergeMessages", () => {
  it("deduplicates by id and keeps the incoming version", () => {
    const existing = [
      directMessage(1),
      directMessage(2, { body: "old" }),
    ];
    const incoming = [
      directMessage(2, { body: "new", client_message_id: "different" }),
    ];

    const merged = mergeMessages(existing, incoming);

    expect(merged.map((message) => message.id)).toEqual([1, 2]);
    expect(merged[1].body).toBe("new");
  });

  it("deduplicates by client_message_id when ids differ", () => {
    const existing = [
      directMessage(1, { client_message_id: "shared-client" }),
    ];
    const incoming = [
      directMessage(2, { client_message_id: "shared-client", body: "new" }),
    ];

    const merged = mergeMessages(existing, incoming);

    expect(merged).toHaveLength(1);
    expect(merged[0].id).toBe(2);
    expect(merged[0].body).toBe("new");
  });

  it("collapses duplicates inside incoming", () => {
    const incoming = [
      directMessage(5, { body: "first" }),
      directMessage(5, { body: "second" }),
      directMessage(6, {
        client_message_id: "direct-client-5",
        body: "third",
      }),
    ];

    const merged = mergeMessages([], incoming);

    expect(merged).toHaveLength(1);
    expect(merged[0]).toMatchObject({ id: 6, body: "third" });
  });

  it("does not collapse distinct messages with empty client ids", () => {
    const merged = mergeMessages(
      [directMessage(1, { client_message_id: "" })],
      [directMessage(2, { client_message_id: "" })],
    );

    expect(merged.map((message) => message.id)).toEqual([1, 2]);
  });

  it("returns a stable ascending order", () => {
    const existing = [
      directMessage(0, { client_message_id: "b" }),
      directMessage(0, { client_message_id: "a" }),
      directMessage(3),
    ];
    const incoming = [directMessage(1)];

    const merged = mergeMessages(existing, incoming);

    expect(merged.map((message) => message.id)).toEqual([0, 0, 1, 3]);
    expect(merged[0].client_message_id).toBe("b");
    expect(merged[1].client_message_id).toBe("a");
  });

  it("does not mutate its inputs", () => {
    const existing = [
      directMessage(1),
      directMessage(0, { client_message_id: "" }),
    ];
    const incoming = [
      directMessage(1, { body: "replacement" }),
      directMessage(2),
    ];
    const existingSnapshot = structuredClone(existing);
    const incomingSnapshot = structuredClone(incoming);

    const merged = mergeMessages(existing, incoming);

    expect(existing).toEqual(existingSnapshot);
    expect(incoming).toEqual(incomingSnapshot);
    expect(merged).not.toBe(existing);
    expect(merged).not.toBe(incoming);
  });
});

describe("sortMessagesByID", () => {
  it("sorts ascending without mutating and is stable for equal ids", () => {
    const first = directMessage(0, { client_message_id: "first" });
    const second = directMessage(0, { client_message_id: "second" });
    const messages = [directMessage(4), second, first, directMessage(2)];
    const snapshot = structuredClone(messages);

    const sorted = sortMessagesByID(messages);

    expect(sorted.map((message) => message.id)).toEqual([0, 0, 2, 4]);
    expect(sorted[0]).toBe(second);
    expect(sorted[1]).toBe(first);
    expect(messages).toEqual(snapshot);
    expect(sorted).not.toBe(messages);
  });
});

describe("trimMessageIDs", () => {
  it("deletes all ids when the limit is zero", () => {
    expect(trimMessageIDs([1, 2, 3], 0)).toEqual([1, 2, 3]);
  });

  it("deletes all ids when the limit is negative", () => {
    expect(trimMessageIDs([1, 2, 3], -5)).toEqual([1, 2, 3]);
  });

  it("deletes nothing when there are fewer ids than the limit", () => {
    expect(trimMessageIDs([1, 2, 3], 5)).toEqual([]);
  });

  it("deletes nothing when the count equals the limit", () => {
    expect(trimMessageIDs([1, 2, 3], 3)).toEqual([]);
  });

  it("deletes the oldest ids when there are more than the limit", () => {
    expect(trimMessageIDs([11, 12, 13, 14, 15], 3)).toEqual([11, 12]);
  });

  it("handles an empty input and does not mutate it", () => {
    const ids = [1, 2, 3, 4];
    const snapshot = ids.slice();

    const deleted = trimMessageIDs(ids, 1);

    expect(deleted).toEqual([1, 2, 3]);
    expect(ids).toEqual(snapshot);
    expect(trimMessageIDs([], 5)).toEqual([]);
  });
});

describe("planGroupSync", () => {
  it("filters unusable, unchanged, and direct conversations", () => {
    const conversations: Conversation[] = [
      groupConversation(1, 20),
      groupConversation(2, null),
      groupConversation(0, 30),
      groupConversation(3, 10),
      groupConversation(4, 40),
      groupConversation(5, 50),
      groupConversation(6, 60),
      groupConversation(7, 70),
      directConversation("bob", 99),
    ];
    const cursors: Record<number, number> = {
      1: 15,
      2: 0,
      0: 0,
      3: 10,
      4: Number.NaN,
      5: -1,
      6: 1.5,
    };

    const plan = planGroupSync(conversations, cursors);

    expect(plan).toEqual([{ groupID: 1, afterID: 15 }]);
  });

  it("deduplicates by group id, keeping the first eligible entry", () => {
    const conversations: Conversation[] = [
      groupConversation(9, 100),
      groupConversation(9, 50),
      groupConversation(8, 90),
    ];

    const plan = planGroupSync(conversations, { 9: 1, 8: 1 });

    expect(plan).toEqual([
      { groupID: 9, afterID: 1 },
      { groupID: 8, afterID: 1 },
    ]);
  });

  it("skips groups without a positive cursor so they load an initial page", () => {
    const conversations: Conversation[] = [
      groupConversation(1, 20),
      groupConversation(2, 30),
    ];

    const plan = planGroupSync(conversations, { 1: 0 });

    expect(plan).toEqual([]);
  });

  it("orders groups by last message id descending", () => {
    const conversations: Conversation[] = [
      groupConversation(1, 5),
      groupConversation(2, 20),
      groupConversation(3, 12),
    ];

    const plan = planGroupSync(conversations, { 1: 1, 2: 1, 3: 1 });

    expect(plan).toEqual([
      { groupID: 2, afterID: 1 },
      { groupID: 3, afterID: 1 },
      { groupID: 1, afterID: 1 },
    ]);
  });

  it("caps the result at the given limit", () => {
    const conversations: Conversation[] = [
      groupConversation(1, 10),
      groupConversation(2, 30),
      groupConversation(3, 20),
    ];
    const cursors: Record<number, number> = { 1: 1, 2: 1, 3: 1 };

    expect(planGroupSync(conversations, cursors, 2)).toEqual([
      { groupID: 2, afterID: 1 },
      { groupID: 3, afterID: 1 },
    ]);
    expect(planGroupSync(conversations, cursors, 0)).toEqual([]);
  });

  it("defaults the cap to 200 groups", () => {
    const conversations: Conversation[] = [];
    const cursors: Record<number, number> = {};
    for (let id = 1; id <= 205; id += 1) {
      conversations.push(groupConversation(id, id));
      cursors[id] = 1;
    }

    const plan = planGroupSync(conversations, cursors);

    expect(plan).toHaveLength(200);
    expect(plan[0]).toEqual({ groupID: 205, afterID: 1 });
    expect(plan[199]).toEqual({ groupID: 6, afterID: 1 });
  });
});
