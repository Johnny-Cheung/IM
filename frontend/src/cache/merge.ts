import type { Conversation } from "../api";
import type { CachedMessage, GroupSyncRequest } from "./types";

function sameMessage(left: CachedMessage, right: CachedMessage): boolean {
  if (left.id !== 0 && left.id === right.id) {
    return true;
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
    for (let index = merged.length - 1; index >= 0; index -= 1) {
      if (sameMessage(merged[index], message)) {
        merged.splice(index, 1);
      }
    }
    merged.push(message);
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
