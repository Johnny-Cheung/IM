import type { FocusTarget } from "./focus";

export const READ_CURSOR_GROUP_INTERVAL_MS = 1000;
export const READ_CURSOR_DIRECT_INTERVAL_MS = 200;

export type ReadCursorReporterOptions = {
  directIntervalMs?: number;
  groupIntervalMs?: number;
  directLeading?: boolean;
};

export type ReadFrame =
  | { type: "read"; kind: "direct"; username: string; last_read_seq: number }
  | { type: "read"; kind: "group"; group_id: number; last_read_seq: number };

export type ReadRowRect = { seq: number; top: number; bottom: number };
export type ReadViewportRect = { top: number; bottom: number };

export function readTargetKey(target: FocusTarget): string {
  return target.kind === "direct"
    ? `direct:${target.username.toLocaleLowerCase()}`
    : `group:${target.groupID}`;
}

// readFrame builds the wire frame for a rendered watermark. A zero or invalid
// seq must never be sent: the server treats through_seq=0 as "advance to the
// conversation counter", which would mark the whole conversation read.
export function readFrame(target: FocusTarget, lastReadSeq: number): ReadFrame | null {
  if (!Number.isSafeInteger(lastReadSeq) || lastReadSeq <= 0) {
    return null;
  }
  if (target.kind === "direct") {
    const username = target.username.trim();
    if (username === "") {
      return null;
    }
    return { type: "read", kind: "direct", username, last_read_seq: lastReadSeq };
  }
  if (!Number.isSafeInteger(target.groupID) || target.groupID <= 0) {
    return null;
  }
  return {
    type: "read",
    kind: "group",
    group_id: target.groupID,
    last_read_seq: lastReadSeq,
  };
}

// lastVisibleReadSeq returns the highest seq whose row intersects the
// viewport. Rows appended while the user is scrolled up are outside the
// viewport and therefore do not advance the watermark.
export function lastVisibleReadSeq(
  viewport: ReadViewportRect,
  rows: readonly ReadRowRect[],
): number {
  let highest = 0;
  for (const row of rows) {
    if (!Number.isSafeInteger(row.seq) || row.seq <= 0) {
      continue;
    }
    if (row.bottom <= viewport.top || row.top >= viewport.bottom) {
      continue;
    }
    if (row.seq > highest) {
      highest = row.seq;
    }
  }
  return highest;
}

// ReadCursorReporter coalesces rendered positions into at most one frame per
// interval. It follows the currently focused conversation only; setTarget
// flushes the previous one before switching so a late frame can never be
// attributed to the new conversation.
export class ReadCursorReporter {
  private target: FocusTarget | null = null;
  private key = "";
  private pendingSeq = 0;
  private latestSeq = 0;
  private lastSentSeq = 0;
  private paused = false;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private lastDirectSendAt = 0;
  private readonly directIntervalMs: number;
  private readonly groupIntervalMs: number;
  private readonly directLeading: boolean;

  constructor(
    private readonly send: (frame: ReadFrame) => boolean,
    options: ReadCursorReporterOptions = {},
  ) {
    this.directIntervalMs =
      options.directIntervalMs ?? READ_CURSOR_DIRECT_INTERVAL_MS;
    this.groupIntervalMs =
      options.groupIntervalMs ?? READ_CURSOR_GROUP_INTERVAL_MS;
    this.directLeading = options.directLeading ?? true;
  }

  // setTarget returns true when the focused conversation changed.
  setTarget(target: FocusTarget | null): boolean {
    const nextKey = target === null ? "" : readTargetKey(target);
    if (nextKey === this.key) {
      this.target = target;
      return false;
    }
    if (!this.paused) {
      this.flushNow();
    }
    this.clearTimer();
    this.target = target;
    this.key = nextKey;
    this.pendingSeq = 0;
    this.latestSeq = 0;
    this.lastSentSeq = 0;
    this.lastDirectSendAt = 0;
    return true;
  }

  observe(seq: number): void {
    if (this.paused || this.target === null) {
      return;
    }
    if (!Number.isSafeInteger(seq) || seq <= 0 || seq <= this.lastSentSeq) {
      return;
    }
    if (seq > this.latestSeq) {
      this.latestSeq = seq;
    }
    if (seq > this.pendingSeq) {
      this.pendingSeq = seq;
    }
    if (this.timer !== null) {
      return;
    }
    // Private chats get a leading edge: the first rendered message after a
    // quiet interval goes out now. Messages arriving in the short trailing
    // window are merged by the timer below.
    if (this.target.kind === "direct" && this.directLeading) {
      if (Date.now() - this.lastDirectSendAt >= this.directIntervalMs) {
        this.flushNow();
      } else {
        this.schedule(this.directIntervalMs);
      }
      return;
    }
    this.schedule(this.intervalForTarget());
  }

  flushNow(): void {
    this.clearTimer();
    if (this.paused || this.target === null || this.pendingSeq <= 0) {
      return;
    }
    const frame = readFrame(this.target, this.pendingSeq);
    if (frame === null) {
      this.pendingSeq = 0;
      return;
    }
    let accepted = false;
    try {
      accepted = this.send(frame);
    } catch {
      accepted = false;
    }
    if (!accepted) {
      // The socket may be closed or the server focus not established yet.
      // Keep the position so a reconnect or focus.ack can replay it.
      this.schedule(this.intervalForTarget());
      return;
    }
    if (this.pendingSeq > this.lastSentSeq) {
      this.lastSentSeq = this.pendingSeq;
    }
    if (this.target.kind === "direct") {
      this.lastDirectSendAt = Date.now();
    }
    this.pendingSeq = 0;
  }

  // noteServerPosition raises the baseline from focus.ack or our own
  // messages.read echo, suppressing cursors the server already covers.
  noteServerPosition(seq: number): void {
    if (!Number.isSafeInteger(seq) || seq <= 0) {
      return;
    }
    if (seq > this.latestSeq) {
      this.latestSeq = seq;
    }
    if (seq > this.lastSentSeq) {
      this.lastSentSeq = seq;
    }
    if (this.pendingSeq <= this.lastSentSeq) {
      this.pendingSeq = 0;
      this.clearTimer();
    }
  }

  pause(): void {
    this.paused = true;
    this.clearTimer();
  }

  resume(): void {
    this.paused = false;
  }

  // transportClosed keeps the newest observed position and clears the sent
  // baseline so the reconnect can replay it after the focus ack.
  transportClosed(): void {
    this.clearTimer();
    this.paused = true;
    this.lastSentSeq = 0;
    this.lastDirectSendAt = 0;
    if (this.latestSeq > this.pendingSeq) {
      this.pendingSeq = this.latestSeq;
    }
  }

  stop(): void {
    this.clearTimer();
    this.paused = true;
    this.target = null;
    this.key = "";
    this.pendingSeq = 0;
    this.latestSeq = 0;
    this.lastSentSeq = 0;
    this.lastDirectSendAt = 0;
  }

  private intervalForTarget(): number {
    return this.target?.kind === "direct"
      ? this.directIntervalMs
      : this.groupIntervalMs;
  }

  private schedule(delayMs: number): void {
    if (this.timer !== null || this.paused) {
      return;
    }
    this.timer = setTimeout(() => {
      this.timer = null;
      this.flushNow();
    }, delayMs);
  }

  private clearTimer(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }
}
