import type { ChatMessage, Conversation, GroupMessage } from "../api";

export function directKey(username: string): string {
  return `direct:${username.toLocaleLowerCase()}`;
}

export function groupKey(groupID: number): string {
  return `group:${groupID}`;
}

export function conversationKey(conversation: Conversation): string {
  return conversation.type === "direct"
    ? directKey(conversation.user.username)
    : groupKey(conversation.group.id);
}

export function groupIDFromKey(key: string): number | null {
  if (!key.startsWith("group:")) {
    return null;
  }
  const parsed = Number(key.slice("group:".length));
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

export function messageConversationKey(
  message: ChatMessage | GroupMessage,
  currentUsername: string,
): string {
  if ("group_id" in message) {
    return groupKey(message.group_id);
  }
  const current = currentUsername.toLocaleLowerCase();
  if (message.sender.username.toLocaleLowerCase() === current) {
    return directKey(message.recipient.username);
  }
  if (message.recipient.username.toLocaleLowerCase() === current) {
    return directKey(message.sender.username);
  }
  // A direct message that does not involve the current user must not be
  // cached under any conversation.
  return "";
}
