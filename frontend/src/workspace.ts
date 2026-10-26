import { api } from "./api";
import type { BootstrapResponse, ChatMessage } from "./api";
import {
  messageConversationKey,
  patchSyncState,
  writeConversations,
  writeFriends,
  writeMessages,
} from "./cache";

const FEED_PAGE_SIZE = 200;
const FEED_DRAIN_ROUNDS = 10;

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

// applyBootstrapToCache persists the first-screen payload. It is independent
// of rendering: callers may start it and forget it.
export async function applyBootstrapToCache(
  payload: BootstrapResponse,
): Promise<void> {
  const user = payload.user.username;

  if (!payload.partial.includes("direct")) {
    await cacheDirectMessages(user, payload.direct.messages);
    await patchSyncState(user, {
      afterID: payload.direct.cursor,
      directComplete: payload.direct.complete,
    });
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
}
