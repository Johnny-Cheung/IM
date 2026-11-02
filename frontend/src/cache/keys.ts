import type { Conversation } from "../api";

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
