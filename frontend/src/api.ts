export type User = {
  username: string;
  nickname: string;
};

export type Relationship =
  | "self"
  | "friend"
  | "incoming_pending"
  | "outgoing_pending"
  | "none";

export type PublicUser = {
  username: string;
  nickname: string;
};

export type UserLookup = PublicUser & {
  relationship: Relationship;
};

export type FriendRequest = {
  id: number;
  user: PublicUser;
  created_at: string;
};

export type FriendRequestStatus = "pending" | "accepted" | "rejected";

export type FriendRequestDirection = "incoming" | "outgoing";

export type FriendRequestRecord = {
  id: number;
  user: PublicUser;
  direction: FriendRequestDirection;
  status: FriendRequestStatus;
  created_at: string;
  handled_at?: string;
};

export type FriendRequests = {
  incoming: FriendRequest[];
  outgoing: FriendRequest[];
  history: FriendRequestRecord[];
};

export type SendFriendRequestResult = {
  status: "pending" | "accepted";
  request_id?: number;
  friend?: PublicUser;
};

export type ChatMessage = {
  id: number;
  seq: number;
  sender: PublicUser;
  recipient: PublicUser;
  body: string;
  client_message_id: string;
  created_at: string;
};

export type ConversationMessage = {
  id: number;
  seq: number;
  group_id?: number;
  sender: PublicUser;
  recipient?: PublicUser;
  body: string;
  client_message_id: string;
  created_at: string;
};

export type DirectConversation = {
  type: "direct";
  user: PublicUser;
  is_friend: boolean;
  last_message: ConversationMessage | null;
  last_seq: number;
  unread_count: number;
  peer_last_read_seq?: number;
};

export type GroupSummary = {
  id: number;
  name: string;
  owner: PublicUser;
  member_count: number;
  created_at: string;
};

export type GroupConversation = {
  type: "group";
  group: GroupSummary;
  is_owner: boolean;
  last_message: ConversationMessage | null;
  last_seq: number;
  unread_count: number;
};

export type Conversation = DirectConversation | GroupConversation;

export type GroupMessage = {
  id: number;
  seq: number;
  group_id: number;
  sender: PublicUser;
  body: string;
  client_message_id: string;
  created_at: string;
};

export type GroupMember = {
  user: PublicUser;
  is_owner: boolean;
  joined_at: string;
};

export type GroupDetail = {
  group: GroupSummary;
  members: GroupMember[];
  is_owner: boolean;
};

export type GroupMessagePageResponse = {
  messages: GroupMessage[];
  has_more: boolean;
};

export type SendGroupMessageResponse = {
  message: GroupMessage;
};

export type GroupReadStats = {
  seq: number;
  message_id: number;
  read_count: number;
  member_count: number;
  all_read: boolean;
};

export type ConversationListResponse = {
  conversations: Conversation[];
};

export type MessagePageResponse = {
  messages: ChatMessage[];
  has_more: boolean;
};

export type SyncMessagePageResponse = {
  messages: ChatMessage[];
  has_more: boolean;
  complete: boolean;
  cursor: number;
};

export type BootstrapGroupCursor = {
  group_id: number;
  after_seq: number;
};

// The private feed is the only cursor that stays on the global message id.
export type BootstrapDirect = {
  messages: ChatMessage[];
  has_more: boolean;
  complete: boolean;
  cursor: number;
};

export type BootstrapGroupDelta = {
  group_id: number;
  messages: GroupMessage[];
  has_more: boolean;
  complete: boolean;
  // Group conversation cursor, in seq units.
  cursor: number;
};

export type BootstrapResponse = {
  user: User;
  conversations: Conversation[];
  friends: PublicUser[];
  direct: BootstrapDirect;
  groups: BootstrapGroupDelta[];
  partial: string[];
};

export type BootstrapInput = {
  afterID: number;
  groups: BootstrapGroupCursor[];
  limit?: number;
};

export type SendMessageResponse = {
  message: ChatMessage;
};

export type RegisterInput = {
  username: string;
  nickname: string;
  password: string;
};

type ErrorPayload = {
  error?: {
    code?: string;
    message?: string;
  };
};

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

export function isAcceptedSeq(value: unknown): value is number {
  return typeof value === "number"
    && Number.isInteger(value)
    && value >= 1
    && value <= Number.MAX_SAFE_INTEGER;
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  if (!headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  let response: Response;
  try {
    response = await fetch(path, {
      ...init,
      credentials: "same-origin",
      headers,
    });
  } catch {
    throw new ApiError(0, "network_error", "无法连接服务器，请检查网络后重试。");
  }

  if (response.status === 204) {
    return undefined as T;
  }

  let payload: unknown = null;
  if ((response.headers.get("Content-Type") ?? "").includes("application/json")) {
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
  }

  if (!response.ok) {
    const body = payload as ErrorPayload | null;
    throw new ApiError(
      response.status,
      body?.error?.code ?? "request_failed",
      body?.error?.message ?? `请求失败（HTTP ${response.status}）。`,
    );
  }

  return payload as T;
}

export const api = {
  me: () => request<User>("/api/me"),

  bootstrap: (input: BootstrapInput) =>
    request<BootstrapResponse>("/api/bootstrap", {
      method: "POST",
      body: JSON.stringify({
        after_id: input.afterID,
        groups: input.groups,
        limit: input.limit ?? 200,
      }),
    }),
  register: (input: RegisterInput) =>
    request<User>("/api/auth/register", {
      method: "POST",
      body: JSON.stringify(input),
    }),

  login: (username: string, password: string) =>
    request<User>("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),

  logout: () =>
    request<void>("/api/auth/logout", {
      method: "POST",
    }),

  updateNickname: (nickname: string) =>
    request<User>("/api/me", {
      method: "PATCH",
      body: JSON.stringify({ nickname }),
    }),

  updatePassword: (oldPassword: string, newPassword: string) =>
    request<void>("/api/me/password", {
      method: "PUT",
      body: JSON.stringify({
        old_password: oldPassword,
        new_password: newPassword,
      }),
    }),

  lookupUser: (username: string) =>
    request<UserLookup>(`/api/users/${encodeURIComponent(username)}`),

  listFriends: () =>
    request<{ friends: PublicUser[] }>("/api/friends"),

  listFriendRequests: () =>
    request<FriendRequests>("/api/friend-requests"),

  sendFriendRequest: (username: string) =>
    request<SendFriendRequestResult>("/api/friend-requests", {
      method: "POST",
      body: JSON.stringify({ username }),
    }),

  acceptFriendRequest: (requestID: number) =>
    request<{ friend: PublicUser }>(`/api/friend-requests/${requestID}/accept`, {
      method: "POST",
    }),

  rejectFriendRequest: (requestID: number) =>
    request<void>(`/api/friend-requests/${requestID}/reject`, {
      method: "POST",
    }),

  deleteFriend: (username: string) =>
    request<void>(`/api/friends/${encodeURIComponent(username)}`, {
      method: "DELETE",
    }),

  listConversations: () =>
    request<ConversationListResponse>("/api/conversations"),

  syncMessages: (afterID: number, limit = 200) => {
    const params = new URLSearchParams();
    params.set("after_id", String(afterID));
    params.set("limit", String(limit));
    return request<SyncMessagePageResponse>(`/api/messages/sync?${params.toString()}`);
  },

  listMessages: (username: string, beforeSeq?: number, limit = 30) => {
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    if (beforeSeq && beforeSeq > 0) {
      params.set("before_seq", String(beforeSeq));
    }
    return request<MessagePageResponse>(
      `/api/conversations/${encodeURIComponent(username)}/messages?${params.toString()}`,
    );
  },

  listMessagesAfter: (username: string, afterSeq: number, limit = 50) => {
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    params.set("after_seq", String(afterSeq));
    return request<MessagePageResponse>(
      `/api/conversations/${encodeURIComponent(username)}/messages?${params.toString()}`,
    );
  },

  sendMessage: (username: string, body: string, clientMessageID: string) =>
    request<SendMessageResponse>(
      `/api/conversations/${encodeURIComponent(username)}/messages`,
      {
        method: "POST",
        body: JSON.stringify({
          body,
          client_message_id: clientMessageID,
        }),
      },
    ),

  createGroup: (name: string, memberUsernames: string[]) =>
    request<GroupDetail>("/api/groups", {
      method: "POST",
      body: JSON.stringify({
        name,
        member_usernames: memberUsernames,
      }),
    }),

  getGroup: (groupID: number) =>
    request<GroupDetail>(`/api/groups/${groupID}`),

  renameGroup: (groupID: number, name: string) =>
    request<GroupDetail>(`/api/groups/${groupID}`, {
      method: "PATCH",
      body: JSON.stringify({ name }),
    }),

  inviteGroupMembers: (groupID: number, usernames: string[]) =>
    request<GroupDetail>(`/api/groups/${groupID}/members`, {
      method: "POST",
      body: JSON.stringify({ usernames }),
    }),

  kickGroupMember: (groupID: number, username: string) =>
    request<GroupDetail>(
      `/api/groups/${groupID}/members/${encodeURIComponent(username)}`,
      { method: "DELETE" },
    ),

  leaveGroup: (groupID: number) =>
    request<{ dissolved: boolean }>(`/api/groups/${groupID}/leave`, {
      method: "POST",
    }),

  listGroupMessages: (groupID: number, beforeSeq?: number, limit = 30) => {
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    if (beforeSeq && beforeSeq > 0) {
      params.set("before_seq", String(beforeSeq));
    }
    return request<GroupMessagePageResponse>(
      `/api/groups/${groupID}/messages?${params.toString()}`,
    );
  },

  listGroupMessagesAfter: (groupID: number, afterSeq: number, limit = 50) => {
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    params.set("after_seq", String(afterSeq));
    return request<GroupMessagePageResponse>(
      `/api/groups/${groupID}/messages?${params.toString()}`,
    );
  },

  groupReadStats: (groupID: number, seq: number) =>
    request<GroupReadStats>(
      `/api/groups/${groupID}/read-stats?seq=${seq}`,
    ),

  sendGroupMessage: (groupID: number, body: string, clientMessageID: string) =>
    request<SendGroupMessageResponse>(
      `/api/groups/${groupID}/messages`,
      {
        method: "POST",
        body: JSON.stringify({
          body,
          client_message_id: clientMessageID,
        }),
      },
    ),
};

export function websocketURL(): string {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}/api/ws`;
}

const sessionErrorCodes = new Set([
  "unauthenticated",
  "session_replaced",
  "session_expired",
  "session_revoked",
  "session_password_changed",
]);

export function isSessionError(error: unknown): error is ApiError {
  return error instanceof ApiError && sessionErrorCodes.has(error.code);
}

export function describeError(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  return "请求失败，请稍后重试。";
}
