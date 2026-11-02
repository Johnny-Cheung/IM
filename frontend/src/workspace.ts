import type { BootstrapResponse } from "./api";
import {
  enforceUserLimits,
  writeConversations,
  writeFriends,
} from "./cache";

// applyBootstrapToCache persists the first-screen metadata. It is independent
// of rendering: callers may start it and forget it. Message bodies are loaded
// lazily when a conversation is opened.
export async function applyBootstrapToCache(
  payload: BootstrapResponse,
): Promise<void> {
  const user = payload.user.username;

  if (!payload.partial.includes("conversations")) {
    await writeConversations(user, payload.conversations);
  }

  if (!payload.partial.includes("friends")) {
    await writeFriends(user, payload.friends);
  }

  await enforceUserLimits(user);
}
