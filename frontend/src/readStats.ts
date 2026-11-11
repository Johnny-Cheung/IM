import type { GroupReadStats } from "./api";

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
