import type {
  ChatMessage,
  Conversation,
  GroupMessage,
  PublicUser,
  User,
} from "../api";

export type CachedMessage = ChatMessage | GroupMessage;

export type SyncState = {
  afterID: number;
  directComplete: boolean;
  groups: Record<number, number>;
  savedAt: string;
};

export type CacheHints = {
  user: User | null;
  sync: SyncState | null;
};

export type GroupSyncRequest = {
  groupID: number;
  afterID: number;
};

export type CachedConversation = {
  user: string;
  key: string;
  conversation: Conversation;
  updatedAt: number;
};

export type CachedFriends = {
  user: string;
  friends: PublicUser[];
  updatedAt: number;
};

export const MESSAGE_LIMIT_PER_CONVERSATION = 200;
export const MESSAGE_LIMIT_PER_USER = 10_000;
export const CONVERSATION_CACHE_LIMIT = 100;

export function emptySyncState(): SyncState {
  return {
    afterID: 0,
    directComplete: true,
    groups: {},
    savedAt: "",
  };
}
