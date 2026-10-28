import type { GroupReadStats } from "./api";

export type ReceiptMessage = {
  id: number;
  seq: number;
  status: string;
  sender: { username: string };
};

export function lastOwnSentSeq(
  messages: readonly ReceiptMessage[],
  username: string,
): number {
  const own = username.toLocaleLowerCase();
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (
      message.status === "sent"
      && Number.isInteger(message.seq)
      && message.seq > 0
      && message.sender.username.toLocaleLowerCase() === own
    ) {
      return message.seq;
    }
  }
  return 0;
}

export function groupReceiptLabel(stats: GroupReadStats): string {
  return stats.all_read
    ? "全部已读"
    : `已读 ${stats.read_count}/${stats.member_count}`;
}
