import { describe, expect, it } from "vitest";
import {
  deserializePendingReads,
  enqueueRead,
  serializePendingReads,
  splitPendingReads,
} from "./readQueue";
import type { PendingRead } from "./readQueue";

describe("read queue", () => {
  it("keeps the highest position per conversation", () => {
    const queue = new Map<string, PendingRead>();
    expect(enqueueRead(queue, { kind: "direct", key: "direct:bob", username: "bob" }, 10)).toBe(true);
    expect(enqueueRead(queue, { kind: "direct", key: "direct:bob", username: "bob" }, 5)).toBe(false);
    expect(enqueueRead(queue, { kind: "direct", key: "direct:bob", username: "bob" }, 10)).toBe(false);
    expect(enqueueRead(queue, { kind: "direct", key: "direct:bob", username: "bob" }, 12)).toBe(true);
    expect(queue.get("direct:bob")?.throughID).toBe(12);
    expect(queue.size).toBe(1);
  });

  it("keeps direct and group conversations independent", () => {
    const queue = new Map<string, PendingRead>();
    enqueueRead(queue, { kind: "direct", key: "direct:bob", username: "bob" }, 10);
    enqueueRead(queue, { kind: "group", key: "group:7", groupID: 7 }, 20);
    expect(queue.size).toBe(2);
    expect(queue.get("direct:bob")?.kind).toBe("direct");
    expect(queue.get("group:7")?.kind).toBe("group");
  });

  it("ignores invalid positions and identities", () => {
    const queue = new Map<string, PendingRead>();
    expect(enqueueRead(queue, { kind: "direct", key: "direct:bob", username: "bob" }, 0)).toBe(false);
    expect(enqueueRead(queue, { kind: "direct", key: "direct:bob", username: "bob" }, -1)).toBe(false);
    expect(enqueueRead(queue, { kind: "direct", key: "direct:bob", username: "bob" }, 1.5)).toBe(false);
    expect(enqueueRead(queue, { kind: "direct", key: "", username: "bob" }, 10)).toBe(false);
    expect(enqueueRead(queue, { kind: "direct", key: "direct:bob", username: "" }, 10)).toBe(false);
    expect(enqueueRead(queue, { kind: "group", key: "group:0", groupID: 0 }, 10)).toBe(false);
    expect(queue.size).toBe(0);
  });

  it("round-trips through serialized storage", () => {
    const queue = new Map<string, PendingRead>();
    enqueueRead(queue, { kind: "direct", key: "direct:bob", username: "bob" }, 12);
    enqueueRead(queue, { kind: "group", key: "group:7", groupID: 7 }, 20);
    const restored = deserializePendingReads(
      JSON.parse(JSON.stringify(serializePendingReads(queue))),
    );
    expect(restored.size).toBe(2);
    expect(restored.get("direct:bob")).toEqual({
      kind: "direct",
      key: "direct:bob",
      username: "bob",
      throughID: 12,
    });
    expect(restored.get("group:7")).toEqual({
      kind: "group",
      key: "group:7",
      groupID: 7,
      throughID: 20,
    });
  });

  it("drops malformed stored entries", () => {
    const restored = deserializePendingReads({
      "direct:bob": { kind: "direct", key: "direct:bob", username: "bob", throughID: 0 },
      "direct:carol": { kind: "direct", key: "different", username: "carol", throughID: 5 },
      "group:7": { kind: "group", key: "group:7", groupID: 7, throughID: 3 },
      "group:bad": { kind: "group", key: "group:bad", groupID: -1, throughID: 3 },
      other: "nope",
    });
    expect([...restored.keys()]).toEqual(["group:7"]);
  });

  it("splits direct and group items", () => {
    const items: PendingRead[] = [
      { kind: "direct", key: "direct:bob", username: "bob", throughID: 1 },
      { kind: "group", key: "group:7", groupID: 7, throughID: 2 },
      { kind: "direct", key: "direct:carol", username: "carol", throughID: 3 },
    ];
    const split = splitPendingReads(items);
    expect(split.direct.map((item) => item.username)).toEqual(["bob", "carol"]);
    expect(split.groups.map((item) => item.groupID)).toEqual([7]);
  });
});
