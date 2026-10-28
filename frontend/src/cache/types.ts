import type {
  ChatMessage,
  Conversation,
  GroupMessage,
  PublicUser,
  User,
} from "../api";

export type CachedMessage = ChatMessage | GroupMessage;

export type SyncState = {
  // Private feed cursor: the only persisted cursor still in global message ids.
  afterID: number;
  directComplete: boolean;
  // Per-group bootstrap cursor, in seq units.
  groups: Record<number, number>;
  // Per-conversation local high-water mark in seq units, keyed by cache key.
  localSeq: Record<string, number>;
  savedAt: string;
};

export type CacheHints = {
  user: User | null;
  sync: SyncState | null;
};

export type GroupSyncRequest = {
  groupID: number;
  afterSeq: number;
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

export type CachedOutbox = {
  user: string;
  conversation: string;
  client_message_id: string;
  body: string;
  kind: "direct" | "group";
  recipient?: PublicUser;
  group_id?: number;
  created_at: string;
  status: "sending" | "failed";
};

export const MESSAGE_LIMIT_PER_CONVERSATION = 200;
export const MESSAGE_LIMIT_PER_USER = 10_000;
export const CONVERSATION_CACHE_LIMIT = 100;

export function emptySyncState(): SyncState {
  return {
    afterID: 0,
    directComplete: true,
    groups: {},
    localSeq: {},
    savedAt: "",
  };
}
