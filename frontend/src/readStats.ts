import type { GroupReadStats } from "./api";

export const READ_STATS_DEBOUNCE_MS = 300;

export type GroupReadEvent = { groupID: number };

export function parseGroupReadEvent(value: unknown): GroupReadEvent | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const event = value as Record<string, unknown>;
  if (event.type !== "group.read") {
    return null;
  }
  const groupID = event.group_id;
  if (typeof groupID !== "number" || !Number.isInteger(groupID) || groupID <= 0) {
    return null;
  }
  return { groupID };
}

export function parseGroupReadStats(value: unknown): GroupReadStats | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const stats = value as Record<string, unknown>;
  const seq = stats.seq;
  const messageID = stats.message_id;
  const readCount = stats.read_count;
  const memberCount = stats.member_count;
  if (
    typeof seq !== "number"
    || !Number.isInteger(seq)
    || seq <= 0
    || typeof messageID !== "number"
    || !Number.isInteger(messageID)
    || messageID <= 0
    || typeof readCount !== "number"
    || typeof memberCount !== "number"
    || typeof stats.all_read !== "boolean"
  ) {
    return null;
  }
  return {
    seq,
    message_id: messageID,
    read_count: readCount,
    member_count: memberCount,
    all_read: stats.all_read,
  };
}

type Timer = ReturnType<typeof setTimeout>;

export class Debouncer {
  private readonly delay: number;
  private timer: Timer | null = null;

  constructor(delay: number) {
    this.delay = delay;
  }

  schedule(run: () => void): void {
    this.cancel();
    this.timer = setTimeout(() => {
      this.timer = null;
      run();
    }, this.delay);
  }

  cancel(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }
}

// A response is only applied while it belongs to the newest request issued
// for the conversation and message seq currently on screen.
export class ReadStatsGate {
  private sequence = 0;
  private groupID = 0;
  private seq = 0;

  reset(): void {
    this.sequence += 1;
    this.groupID = 0;
    this.seq = 0;
  }

  begin(groupID: number, seq: number): number {
    this.sequence += 1;
    this.groupID = groupID;
    this.seq = seq;
    return this.sequence;
  }

  accepts(token: number, groupID: number, seq: number): boolean {
    return token === this.sequence
      && groupID === this.groupID
      && seq === this.seq;
  }
}
