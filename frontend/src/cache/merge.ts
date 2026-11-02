import type { CachedMessage } from "./types";

function acceptedSeq(message: CachedMessage): number {
  return Number.isInteger(message.seq) && message.seq > 0 ? message.seq : 0;
}

function sameMessage(left: CachedMessage, right: CachedMessage): boolean {
  const leftSeq = acceptedSeq(left);
  const rightSeq = acceptedSeq(right);
  if (leftSeq > 0 && rightSeq > 0) {
    // Two confirmed messages are the same only when they share a conversation
    // seq; global ids are never compared across timelines.
    return leftSeq === rightSeq;
  }
  return (
    left.client_message_id !== "" &&
    left.client_message_id === right.client_message_id
  );
}

// Confirmed messages order by seq; local pending messages (seq 0) sort last.
export function sortMessagesBySeq(messages: CachedMessage[]): CachedMessage[] {
  return messages.slice().sort((left, right) => {
    const leftSeq = acceptedSeq(left) || Number.MAX_SAFE_INTEGER;
    const rightSeq = acceptedSeq(right) || Number.MAX_SAFE_INTEGER;
    if (leftSeq !== rightSeq) {
      return leftSeq - rightSeq;
    }
    if (acceptedSeq(left) === 0 && acceptedSeq(right) === 0) {
      const byClient = left.client_message_id.localeCompare(right.client_message_id);
      if (byClient !== 0) {
        return byClient;
      }
    }
    return left.id - right.id;
  });
}

export type MessageMergeResult = {
  messages: CachedMessage[];
  // Lowest seq where the incoming message reuses a cached seq under another
  // global id. The cached tail from here on is stale and must be discarded.
  conflictSeq: number | null;
};

export function mergeMessages(
  existing: CachedMessage[],
  incoming: CachedMessage[],
): MessageMergeResult {
  let conflictSeq: number | null = null;
  const merged = existing.slice();

  for (const message of incoming) {
    const index = merged.findIndex((item) => sameMessage(item, message));
    const seq = acceptedSeq(message);
    if (index < 0) {
      merged.push(message);
      continue;
    }
    const current = merged[index];
    const currentSeq = acceptedSeq(current);
    if (currentSeq > 0 && seq > 0 && current.id !== message.id) {
      conflictSeq = conflictSeq === null
        ? currentSeq
        : Math.min(conflictSeq, currentSeq);
    }
    // Never let an unconfirmed copy replace a server-confirmed one.
    if (seq > 0 || message.id >= current.id) {
      merged[index] = message;
    }
  }

  let kept = merged;
  if (conflictSeq !== null) {
    const floor = conflictSeq;
    kept = merged.filter((message) => {
      const seq = acceptedSeq(message);
      return seq === 0 || seq < floor || incoming.includes(message);
    });
  }

  const bySeq = new Map<number, CachedMessage>();
  const deduped: CachedMessage[] = [];
  for (const message of kept) {
    const seq = acceptedSeq(message);
    if (seq === 0) {
      deduped.push(message);
      continue;
    }
    const previous = bySeq.get(seq);
    if (previous === undefined) {
      bySeq.set(seq, message);
      deduped.push(message);
      continue;
    }
    if (previous.id !== message.id) {
      conflictSeq = conflictSeq === null
        ? seq
        : Math.min(conflictSeq, seq);
    }
    if (message.id >= previous.id) {
      const position = deduped.indexOf(previous);
      deduped[position] = message;
      bySeq.set(seq, message);
    }
  }

  return { messages: sortMessagesBySeq(deduped), conflictSeq };
}
