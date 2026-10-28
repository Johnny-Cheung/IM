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
  sortMessagesBySeq,
  trimMessageIDs,
} from "./merge";

const alice: PublicUser = { username: "alice", nickname: "Alice" };
const bob: PublicUser = { username: "bob", nickname: "Bob" };

function directMessage(
  id: number,
  seq: number = id,
  overrides: Partial<ChatMessage> = {},
): ChatMessage {
  return {
    id,
    seq,
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
  lastSeq: number | null,
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
      lastSeq === null
        ? null
        : {
            id: lastSeq * 10,
            seq: lastSeq,
            group_id: id,
            sender: alice,
            body: `group-${lastSeq}`,
            client_message_id: `group-client-${lastSeq}`,
            created_at: "2026-10-26T00:00:00Z",
          },
    last_seq: lastSeq ?? 0,
    unread_count: 0,
  };
}

function directConversation(
  username: string,
  lastSeq: number | null,
): DirectConversation {
  const peer: PublicUser = { username, nickname: username };
  return {
    type: "direct",
    user: peer,
    is_friend: true,
    last_message:
      lastSeq === null
        ? null
        : {
            id: lastSeq * 10,
            seq: lastSeq,
            sender: alice,
            recipient: peer,
            body: `direct-${lastSeq}`,
            client_message_id: `direct-client-${lastSeq}`,
            created_at: "2026-10-26T00:00:00Z",
          },
    last_seq: lastSeq ?? 0,
    unread_count: 0,
    peer_last_read_seq: 0,
  };
}

describe("mergeMessages", () => {
  it("deduplicates by seq and keeps the incoming version", () => {
    const existing = [
      directMessage(1),
      directMessage(2, 2, { body: "old" }),
    ];
    const incoming = [
      directMessage(2, 2, { body: "new", client_message_id: "different" }),
    ];

    const { messages, conflictSeq } = mergeMessages(existing, incoming);

    expect(messages.map((message) => message.seq)).toEqual([1, 2]);
    expect(messages[1].body).toBe("new");
    expect(conflictSeq).toBeNull();
  });

  it("keeps two confirmed messages even when client ids collide", () => {
    const existing = [
      directMessage(1, 1, { client_message_id: "shared-client" }),
    ];
    const incoming = [
      directMessage(2, 2, { client_message_id: "shared-client", body: "new" }),
    ];

    const { messages } = mergeMessages(existing, incoming);

    // Client message ids are only unique per sender, so two server-confirmed
    // messages must never collapse into one.
    expect(messages.map((message) => message.seq)).toEqual([1, 2]);
  });

  it("does not let an unconfirmed copy replace a confirmed message", () => {
    const existing = [
      directMessage(1, 1, { client_message_id: "shared-client", body: "confirmed" }),
    ];
    const incoming = [
      directMessage(0, 0, { client_message_id: "shared-client", body: "optimistic" }),
    ];

    const { messages } = mergeMessages(existing, incoming);

    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({ id: 1, body: "confirmed" });
  });

  it("replaces a pending copy with its confirmed message", () => {
    const pending = directMessage(0, 0, {
      client_message_id: "pending",
      body: "pending",
    });
    const confirmed = directMessage(9, 4, {
      client_message_id: "pending",
      body: "confirmed",
    });

    const { messages } = mergeMessages([pending], [confirmed]);

    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({ id: 9, seq: 4, body: "confirmed" });
  });

  it("collapses exact duplicates inside incoming", () => {
    const incoming = [
      directMessage(5, 5, { body: "first" }),
      directMessage(5, 5, { body: "second" }),
    ];

    const { messages } = mergeMessages([], incoming);

    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({ id: 5, body: "second" });
  });

  it("does not collapse distinct messages with empty client ids", () => {
    const { messages } = mergeMessages(
      [directMessage(1, 1, { client_message_id: "" })],
      [directMessage(2, 2, { client_message_id: "" })],
    );

    expect(messages.map((message) => message.seq)).toEqual([1, 2]);
  });

  it("orders confirmed messages by seq and pending messages last", () => {
    const existing = [
      directMessage(0, 0, { client_message_id: "b" }),
      directMessage(0, 0, { client_message_id: "a" }),
      directMessage(30, 3),
    ];
    const incoming = [directMessage(10, 1)];

    const { messages } = mergeMessages(existing, incoming);

    expect(messages.map((message) => message.seq)).toEqual([1, 3, 0, 0]);
    expect(messages[2].client_message_id).toBe("a");
    expect(messages[3].client_message_id).toBe("b");
  });

  it("reports a reused seq and drops the stale tail above it", () => {
    const existing = [
      directMessage(10, 1),
      directMessage(20, 2, { body: "stale-2" }),
      directMessage(30, 3, { body: "stale-3" }),
    ];
    const incoming = [
      directMessage(99, 2, { body: "server-2" }),
    ];

    const { messages, conflictSeq } = mergeMessages(existing, incoming);

    expect(conflictSeq).toBe(2);
    expect(messages.map((message) => message.seq)).toEqual([1, 2]);
    expect(messages[1].body).toBe("server-2");
  });

  it("does not mutate its inputs", () => {
    const existing = [
      directMessage(1),
      directMessage(0, 0, { client_message_id: "" }),
    ];
    const incoming = [
      directMessage(1, 1, { body: "replacement" }),
      directMessage(2),
    ];
    const existingSnapshot = structuredClone(existing);
    const incomingSnapshot = structuredClone(incoming);

    const { messages } = mergeMessages(existing, incoming);

    expect(existing).toEqual(existingSnapshot);
    expect(incoming).toEqual(incomingSnapshot);
    expect(messages).not.toBe(existing);
    expect(messages).not.toBe(incoming);
  });
});

describe("sortMessagesBySeq", () => {
  it("sorts ascending by seq without mutating and keeps pending messages last", () => {
    const first = directMessage(0, 0, { client_message_id: "first" });
    const second = directMessage(0, 0, { client_message_id: "second" });
    const messages = [directMessage(40, 4), second, first, directMessage(20, 2)];
    const snapshot = structuredClone(messages);

    const sorted = sortMessagesBySeq(messages);

    expect(sorted.map((message) => message.seq)).toEqual([2, 4, 0, 0]);
    expect(sorted[2]).toBe(first);
    expect(sorted[3]).toBe(second);
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

    expect(plan).toEqual([{ groupID: 1, afterSeq: 15 }]);
  });

  it("deduplicates by group id, keeping the first eligible entry", () => {
    const conversations: Conversation[] = [
      groupConversation(9, 100),
      groupConversation(9, 50),
      groupConversation(8, 90),
    ];

    const plan = planGroupSync(conversations, { 9: 1, 8: 1 });

    expect(plan).toEqual([
      { groupID: 9, afterSeq: 1 },
      { groupID: 8, afterSeq: 1 },
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

  it("orders groups by last seq descending", () => {
    const conversations: Conversation[] = [
      groupConversation(1, 5),
      groupConversation(2, 20),
      groupConversation(3, 12),
    ];

    const plan = planGroupSync(conversations, { 1: 1, 2: 1, 3: 1 });

    expect(plan).toEqual([
      { groupID: 2, afterSeq: 1 },
      { groupID: 3, afterSeq: 1 },
      { groupID: 1, afterSeq: 1 },
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
      { groupID: 2, afterSeq: 1 },
      { groupID: 3, afterSeq: 1 },
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
    expect(plan[0]).toEqual({ groupID: 205, afterSeq: 1 });
    expect(plan[199]).toEqual({ groupID: 6, afterSeq: 1 });
  });
});
