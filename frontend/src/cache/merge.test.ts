import { describe, expect, it } from "vitest";
import type { ChatMessage, PublicUser } from "../api";
import { mergeMessages, sortMessagesBySeq } from "./merge";

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
