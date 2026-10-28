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
  const messageID = stats.message_id;
  const readCount = stats.read_count;
  const memberCount = stats.member_count;
  if (
    typeof messageID !== "number"
    || typeof readCount !== "number"
    || typeof memberCount !== "number"
    || typeof stats.all_read !== "boolean"
  ) {
    return null;
  }
  return {
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
// for the conversation and message currently on screen.
export class ReadStatsGate {
  private sequence = 0;
  private groupID = 0;
  private messageID = 0;

  reset(): void {
    this.sequence += 1;
    this.groupID = 0;
    this.messageID = 0;
  }

  begin(groupID: number, messageID: number): number {
    this.sequence += 1;
    this.groupID = groupID;
    this.messageID = messageID;
    return this.sequence;
  }

  accepts(token: number, groupID: number, messageID: number): boolean {
    return token === this.sequence
      && groupID === this.groupID
      && messageID === this.messageID;
  }
}
