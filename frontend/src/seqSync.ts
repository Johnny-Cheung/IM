// Pure ordering rules for conversation-local message seqs. Nothing in this
// module touches React, the network or IndexedDB, so every rule the design
// states for incoming messages can be unit-tested directly.

export type IncomingPlan =
  | { kind: "append" }
  | { kind: "duplicate" }
  | { kind: "pull"; afterSeq: number; targetSeq: number }
  | { kind: "resync"; fromSeq: number };

export type LocalSeqSnapshot = {
  maxSeq: number;
  seqIDs?: ReadonlyMap<number, number>;
};

export function isAcceptedMessageSeq(value: unknown): value is number {
  return typeof value === "number"
    && Number.isInteger(value)
    && value >= 1
    && value <= Number.MAX_SAFE_INTEGER;
}

// Local pending messages carry seq 0 and never take part in comparisons.
export function maxAcceptedSeq(
  messages: readonly { seq: number }[],
): number {
  let max = 0;
  for (const message of messages) {
    if (isAcceptedMessageSeq(message.seq) && message.seq > max) {
      max = message.seq;
    }
  }
  return max;
}

export function acceptedSeqMap(
  messages: readonly { seq: number; id: number }[],
): Map<number, number> {
  const map = new Map<number, number>();
  for (const message of messages) {
    if (isAcceptedMessageSeq(message.seq) && message.id > 0) {
      map.set(message.seq, message.id);
    }
  }
  return map;
}

export function planIncoming(
  seq: number,
  id: number,
  local: LocalSeqSnapshot,
): IncomingPlan {
  if (!isAcceptedMessageSeq(seq) || id <= 0) {
    return { kind: "duplicate" };
  }
  const maxSeq = isAcceptedMessageSeq(local.maxSeq) ? local.maxSeq : 0;
  if (maxSeq === 0) {
    return seq === 1
      ? { kind: "append" }
      : { kind: "pull", afterSeq: 0, targetSeq: seq };
  }
  if (seq === maxSeq + 1) {
    return { kind: "append" };
  }
  if (seq > maxSeq + 1) {
    return { kind: "pull", afterSeq: maxSeq, targetSeq: seq };
  }
  const knownID = local.seqIDs?.get(seq);
  // A seq below the watermark has already been seen; without a cached id to
  // compare it stays a duplicate instead of triggering a resync storm.
  if (knownID === undefined || knownID === id) {
    return { kind: "duplicate" };
  }
  return { kind: "resync", fromSeq: seq };
}

// The server's conversation last_seq is below the local watermark: the server
// rolled back. Everything from serverLastSeq + 1 on is stale locally.
export function planRollback(
  serverLastSeq: number,
  localMaxSeq: number,
): { fromSeq: number } | null {
  const server = Number.isInteger(serverLastSeq) && serverLastSeq > 0
    ? serverLastSeq
    : 0;
  const local = isAcceptedMessageSeq(localMaxSeq) ? localMaxSeq : 0;
  if (local <= server) {
    return null;
  }
  return { fromSeq: server + 1 };
}

// A conversation absent from the list has no last_seq at all; that is not a
// server position of 0 and must never be read as a rollback.
export function planListRollback(
  serverLastSeq: number | null | undefined,
  localMaxSeq: number,
): { fromSeq: number } | null {
  if (
    serverLastSeq === undefined
    || serverLastSeq === null
    || !Number.isInteger(serverLastSeq)
    || serverLastSeq < 0
  ) {
    return null;
  }
  return planRollback(serverLastSeq, localMaxSeq);
}

export type ActiveSyncPlan =
  | { kind: "initial" }
  | { kind: "resync"; fromSeq: number }
  | { kind: "catch-up"; afterSeq: number };

// Reconnect sync compares only the tail message the list reported. The list
// last_seq itself is a snapshot that can trail a tail this tab already pulled,
// so it never proves a rollback here; a fresh list response decides that.
export function planActiveSync(
  newestSeq: number,
  newestMessage: { id: number; seq: number } | undefined,
  serverLast: { id: number; seq: number } | undefined,
): ActiveSyncPlan {
  const newest = isAcceptedMessageSeq(newestSeq) ? newestSeq : 0;
  if (newest === 0) {
    return { kind: "initial" };
  }
  if (
    newestMessage !== undefined
    && serverLast !== undefined
    && serverLast.seq === newest
    && serverLast.id !== newestMessage.id
  ) {
    return { kind: "resync", fromSeq: newest };
  }
  return { kind: "catch-up", afterSeq: newest };
}

export type OrderableMessage = {
  id: number;
  seq: number;
  client_message_id: string;
  created_at: string;
};

// Confirmed messages order by seq. Pending and failed messages (seq 0) keep
// send order via created_at, with the client id only as a tie-break.
export function sortConversationMessages<T extends OrderableMessage>(
  messages: readonly T[],
): T[] {
  return [...messages].sort((left, right) => {
    const leftSeq = isAcceptedMessageSeq(left.seq)
      ? left.seq
      : Number.MAX_SAFE_INTEGER;
    const rightSeq = isAcceptedMessageSeq(right.seq)
      ? right.seq
      : Number.MAX_SAFE_INTEGER;
    if (leftSeq !== rightSeq) {
      return leftSeq - rightSeq;
    }
    if (!isAcceptedMessageSeq(left.seq) && !isAcceptedMessageSeq(right.seq)) {
      const byTime = left.created_at.localeCompare(right.created_at);
      if (byTime !== 0) {
        return byTime;
      }
      const byClient = left.client_message_id.localeCompare(
        right.client_message_id,
      );
      if (byClient !== 0) {
        return byClient;
      }
    }
    return left.id - right.id;
  });
}

export function missingSeqs(
  afterSeq: number,
  targetSeq: number,
  present: Iterable<number>,
  limit = 500,
): number[] {
  if (!isAcceptedMessageSeq(targetSeq) || limit <= 0) {
    return [];
  }
  const seen = new Set<number>();
  for (const seq of present) {
    if (isAcceptedMessageSeq(seq)) {
      seen.add(seq);
    }
  }
  const missing: number[] = [];
  const start = Number.isInteger(afterSeq) && afterSeq > 0 ? afterSeq + 1 : 1;
  for (let seq = start; seq <= targetSeq && missing.length < limit; seq += 1) {
    if (!seen.has(seq)) {
      missing.push(seq);
    }
  }
  return missing;
}

export type GapPullPlan = { afterSeq: number; targetSeq: number };

// A seq already marked missing is never retried; the pull resumes after the
// highest skipped seq so the server keeps making progress.
export function planGapPull(
  afterSeq: number,
  targetSeq: number,
  present: Iterable<number>,
  knownMissing: ReadonlySet<number> = new Set(),
): GapPullPlan | null {
  const unfilled = missingSeqs(afterSeq, targetSeq, present)
    .filter((seq) => !knownMissing.has(seq));
  if (unfilled.length === 0) {
    return null;
  }
  const first = unfilled[0];
  let cursor = Number.isInteger(afterSeq) && afterSeq > 0 ? afterSeq : 0;
  for (const seq of knownMissing) {
    if (seq > cursor && seq < first) {
      cursor = seq;
    }
  }
  return { afterSeq: cursor, targetSeq };
}

export function nextUnread(
  plan: IncomingPlan,
  currentUnread: number,
  fromSelf: boolean,
): { unread: number; refresh: boolean } {
  const unread = Number.isFinite(currentUnread)
    ? Math.max(0, Math.trunc(currentUnread))
    : 0;
  if (plan.kind === "append") {
    return { unread: fromSelf ? unread : unread + 1, refresh: false };
  }
  if (plan.kind === "pull" || plan.kind === "resync") {
    return { unread, refresh: true };
  }
  return { unread, refresh: false };
}

// The conversation preview only moves forward; stale or duplicate pushes must
// not roll it back.
export function shouldReplacePreview(
  conversationLastSeq: number,
  incomingSeq: number,
): boolean {
  if (!isAcceptedMessageSeq(incomingSeq)) {
    return false;
  }
  const current = Number.isInteger(conversationLastSeq) && conversationLastSeq > 0
    ? conversationLastSeq
    : 0;
  return incomingSeq > current;
}

export function isReadByPeer(
  messageSeq: number,
  peerLastReadSeq: number,
): boolean {
  return isAcceptedMessageSeq(messageSeq)
    && Number.isInteger(peerLastReadSeq)
    && peerLastReadSeq >= messageSeq;
}

// Outbox rows whose client id never came back confirmed were lost with the
// server's dropped tail; they surface as failed and can be retried.
export function lostOutboxEntries<T extends { client_message_id: string }>(
  entries: readonly T[],
  confirmedClientIDs: ReadonlySet<string>,
): T[] {
  return entries.filter((entry) => !confirmedClientIDs.has(entry.client_message_id));
}

export class MissingSeqTracker {
  private readonly byConversation = new Map<string, Set<number>>();

  mark(conversation: string, seqs: Iterable<number>): void {
    const missing = this.byConversation.get(conversation) ?? new Set<number>();
    for (const seq of seqs) {
      if (isAcceptedMessageSeq(seq)) {
        missing.add(seq);
      }
    }
    if (missing.size > 0) {
      this.byConversation.set(conversation, missing);
    }
  }

  has(conversation: string, seq: number): boolean {
    return this.byConversation.get(conversation)?.has(seq) ?? false;
  }

  pending(conversation: string): ReadonlySet<number> {
    return this.byConversation.get(conversation) ?? EMPTY_SET;
  }

  clear(conversation: string): void {
    this.byConversation.delete(conversation);
  }
}

const EMPTY_SET: ReadonlySet<number> = new Set<number>();
