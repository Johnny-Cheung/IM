import type { Conversation } from "../api";
import type { CachedMessage, GroupSyncRequest } from "./types";

function sameMessage(left: CachedMessage, right: CachedMessage): boolean {
  if (left.id > 0 && right.id > 0) {
    // Two confirmed messages are the same only when the server says so. Client
    // message ids are not unique across senders, so they must not merge them.
    return left.id === right.id;
  }
  return (
    left.client_message_id !== "" &&
    left.client_message_id === right.client_message_id
  );
}

export function sortMessagesByID(messages: CachedMessage[]): CachedMessage[] {
  return messages.slice().sort((left, right) => left.id - right.id);
}

export function mergeMessages(
  existing: CachedMessage[],
  incoming: CachedMessage[],
): CachedMessage[] {
  const merged = existing.slice();
  for (const message of incoming) {
    let replaced = false;
    for (let index = 0; index < merged.length; index += 1) {
      if (!sameMessage(merged[index], message)) {
        continue;
      }
      // Never let an unconfirmed copy replace a server-confirmed one.
      if (message.id >= merged[index].id) {
        merged[index] = message;
      }
      replaced = true;
      break;
    }
    if (!replaced) {
      merged.push(message);
    }
  }
  return sortMessagesByID(merged);
}

export function trimMessageIDs(ids: number[], limit: number): number[] {
  if (limit <= 0) {
    return ids.slice();
  }
  const removeCount = Math.max(0, ids.length - Math.floor(limit));
  return ids.slice(0, removeCount);
}

// planGroupSync must be called with a freshly fetched conversation list. A
// cached list is stale by definition and would drop the groups that received
// messages since the last session.
export function planGroupSync(
  conversations: Conversation[],
  cursors: Record<number, number>,
  limit = 200,
): GroupSyncRequest[] {
  if (limit <= 0) {
    return [];
  }

  const seen = new Set<number>();
  const plans: Array<GroupSyncRequest & { lastMessageID: number }> = [];

  for (const conversation of conversations) {
    if (conversation.type !== "group") {
      continue;
    }
    const groupID = conversation.group.id;
    if (groupID <= 0) {
      continue;
    }
    const lastMessage = conversation.last_message;
    if (lastMessage === null) {
      continue;
    }
    const cursor = cursors[groupID];
    if (!Number.isInteger(cursor) || cursor <= 0) {
      continue;
    }
    if (lastMessage.id <= cursor || seen.has(groupID)) {
      continue;
    }
    seen.add(groupID);
    plans.push({
      groupID,
      afterID: cursor,
      lastMessageID: lastMessage.id,
    });
  }

  plans.sort((left, right) => right.lastMessageID - left.lastMessageID);
  return plans
    .slice(0, limit)
    .map(({ groupID, afterID }) => ({ groupID, afterID }));
}
