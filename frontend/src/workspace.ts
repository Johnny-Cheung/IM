import { api } from "./api";
import type {
  BootstrapGroupDelta,
  BootstrapResponse,
  ChatMessage,
} from "./api";
import {
  enforceUserLimits,
  groupKey,
  messageConversationKey,
  patchSyncState,
  removeConversation,
  setGroupCursor,
  writeConversations,
  writeFriends,
  writeMessages,
} from "./cache";

const FEED_PAGE_SIZE = 200;
const FEED_DRAIN_ROUNDS = 10;
const GROUP_PAGE_SIZE = 50;
const GROUP_DRAIN_ROUNDS = 4;

async function cacheDirectMessages(
  user: string,
  messages: ChatMessage[],
): Promise<void> {
  const grouped = new Map<string, ChatMessage[]>();
  for (const message of messages) {
    const key = messageConversationKey(message, user);
    if (key === "") {
      continue;
    }
    const bucket = grouped.get(key) ?? [];
    bucket.push(message);
    grouped.set(key, bucket);
  }
  await Promise.all(
    Array.from(grouped.entries()).map(([key, bucket]) =>
      writeMessages(user, key, bucket)),
  );
}

async function drainDirectFeed(
  user: string,
  startCursor: number,
): Promise<void> {
  // The private feed cursor is a global message id and is the only cursor that
  // must not be converted to a conversation seq.
  let cursor = startCursor;
  for (let round = 0; round < FEED_DRAIN_ROUNDS; round += 1) {
    let page;
    try {
      page = await api.syncMessages(cursor, FEED_PAGE_SIZE);
    } catch {
      return;
    }
    await cacheDirectMessages(user, page.messages);
    await patchSyncState(user, {
      afterID: page.cursor,
      directComplete: page.complete,
    });
    if (!page.complete || !page.has_more || page.cursor <= cursor) {
      return;
    }
    cursor = page.cursor;
  }
}

async function drainGroupFeed(
  user: string,
  groupID: number,
  startCursor: number,
): Promise<void> {
  let cursor = startCursor;
  for (let round = 0; round < GROUP_DRAIN_ROUNDS; round += 1) {
    let page;
    try {
      page = await api.listGroupMessagesAfter(groupID, cursor, GROUP_PAGE_SIZE);
    } catch {
      return;
    }
    if (page.messages.length === 0) {
      return;
    }
    await writeMessages(user, groupKey(groupID), page.messages);
    // Group drains page and advance by conversation seq.
    const newest = page.messages.reduce(
      (latest, message) => (message.seq > latest ? message.seq : latest),
      cursor,
    );
    if (newest <= cursor) {
      return;
    }
    await setGroupCursor(user, groupID, newest);
    cursor = newest;
    if (!page.has_more) {
      return;
    }
  }
}

async function cacheGroupDeltas(
  user: string,
  deltas: BootstrapGroupDelta[],
): Promise<void> {
  for (const delta of deltas) {
    const key = groupKey(delta.group_id);
    if (!delta.complete) {
      // The delta does not cover the cached cursor. Drop the local copy so the
      // next open loads a fresh page instead of stitching over a gap.
      await removeConversation(user, key);
      continue;
    }
    await writeMessages(user, key, delta.messages);
    await setGroupCursor(user, delta.group_id, delta.cursor);
    if (delta.has_more) {
      await drainGroupFeed(user, delta.group_id, delta.cursor);
    }
  }
}

// applyBootstrapToCache persists the first-screen payload. It is independent
// of rendering: callers may start it and forget it.
export async function applyBootstrapToCache(
  payload: BootstrapResponse,
  previousAfterID = 0,
): Promise<void> {
  const user = payload.user.username;

  if (!payload.partial.includes("direct")) {
    await cacheDirectMessages(user, payload.direct.messages);
    await patchSyncState(user, {
      afterID: payload.direct.cursor,
      // With no previous sync point there is no local cache to distrust: the
      // feed gap lies below the index epoch and was never cached.
      directComplete: payload.direct.complete || previousAfterID === 0,
    });
  }

  if (!payload.partial.includes("groups") && payload.groups.length > 0) {
    await cacheGroupDeltas(user, payload.groups);
  }

  if (!payload.partial.includes("conversations")) {
    // Runs after the messages so conversations that disappeared on the server
    // also lose their cached messages.
    await writeConversations(user, payload.conversations);
  }

  if (!payload.partial.includes("friends")) {
    await writeFriends(user, payload.friends);
  }

  if (!payload.partial.includes("direct") && payload.direct.has_more) {
    await drainDirectFeed(user, payload.direct.cursor);
  }

  await enforceUserLimits(user);
}
