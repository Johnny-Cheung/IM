import { describe, expect, it } from "vitest";
import { BLUR_FRAME, focusFrame, parseFocusAck } from "./focus";

describe("focus frames", () => {
  it("builds a direct focus frame", () => {
    expect(focusFrame({ kind: "direct", username: "bob" })).toEqual({
      type: "focus",
      kind: "direct",
      username: "bob",
    });
  });

  it("builds a group focus frame", () => {
    expect(focusFrame({ kind: "group", groupID: 12 })).toEqual({
      type: "focus",
      kind: "group",
      group_id: 12,
    });
  });

  it("keeps the blur frame constant", () => {
    expect(BLUR_FRAME).toEqual({ type: "blur" });
  });
});

describe("focus acknowledgements", () => {
  it("parses a direct acknowledgement", () => {
    expect(parseFocusAck({
      type: "focus.ack",
      kind: "direct",
      username: "bob",
      last_read_seq: 34,
    })).toEqual({
      ok: true,
      target: { kind: "direct", username: "bob" },
      lastReadSeq: 34,
    });
  });

  it("parses a group acknowledgement", () => {
    expect(parseFocusAck({
      type: "focus.ack",
      kind: "group",
      group_id: 12,
      last_read_seq: 0,
    })).toEqual({
      ok: true,
      target: { kind: "group", groupID: 12 },
      lastReadSeq: 0,
    });
  });

  it("reports an error acknowledgement without a position", () => {
    expect(parseFocusAck({
      type: "focus.ack",
      kind: "group",
      group_id: 12,
      error: "not_group_member",
    })).toEqual({ ok: false, error: "not_group_member" });
  });

  it("drops malformed acknowledgements", () => {
    expect(parseFocusAck(null)).toBeNull();
    expect(parseFocusAck({ type: "focus.ack" })).toBeNull();
    expect(parseFocusAck({
      type: "focus.ack",
      kind: "direct",
      username: "",
      last_read_seq: 1,
    })).toBeNull();
    expect(parseFocusAck({
      type: "focus.ack",
      kind: "group",
      group_id: 0,
      last_read_seq: 1,
    })).toBeNull();
    expect(parseFocusAck({
      type: "focus.ack",
      kind: "group",
      group_id: 4,
    })).toBeNull();
    expect(parseFocusAck({
      type: "focus.ack",
      kind: "direct",
      username: "bob",
      last_read_message_id: 34,
    })).toBeNull();
    expect(parseFocusAck({ type: "group.read", group_id: 4 })).toBeNull();
  });
});
