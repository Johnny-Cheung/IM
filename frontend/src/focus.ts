export type FocusTarget =
  | { kind: "direct"; username: string }
  | { kind: "group"; groupID: number };

export type FocusFrame =
  | { type: "focus"; kind: "direct"; username: string }
  | { type: "focus"; kind: "group"; group_id: number }
  | { type: "blur" };

export type FocusAck =
  | { ok: true; target: FocusTarget; lastReadSeq: number }
  | { ok: false; error: string };

export const BLUR_FRAME: FocusFrame = { type: "blur" };

export function focusFrame(target: FocusTarget): FocusFrame {
  return target.kind === "direct"
    ? { type: "focus", kind: "direct", username: target.username }
    : { type: "focus", kind: "group", group_id: target.groupID };
}

function ackError(event: Record<string, unknown>): string {
  return typeof event.error === "string" ? event.error : "";
}

function ackPosition(event: Record<string, unknown>): number | null {
  const position = event.last_read_seq;
  return typeof position === "number" && Number.isSafeInteger(position) && position >= 0
    ? position
    : null;
}

export function parseFocusAck(value: unknown): FocusAck | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const event = value as Record<string, unknown>;
  if (event.type !== "focus.ack") {
    return null;
  }
  const error = ackError(event);
  const position = ackPosition(event);
  if (event.kind === "direct") {
    if (typeof event.username !== "string" || event.username === "") {
      return null;
    }
    if (error !== "") {
      return { ok: false, error };
    }
    if (position === null) {
      return null;
    }
    return {
      ok: true,
      target: { kind: "direct", username: event.username },
      lastReadSeq: position,
    };
  }
  if (event.kind === "group") {
    const groupID = event.group_id;
    if (typeof groupID !== "number" || !Number.isInteger(groupID) || groupID <= 0) {
      return null;
    }
    if (error !== "") {
      return { ok: false, error };
    }
    if (position === null) {
      return null;
    }
    return {
      ok: true,
      target: { kind: "group", groupID },
      lastReadSeq: position,
    };
  }
  return null;
}
