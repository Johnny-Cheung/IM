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
  sender: PublicUser;
  recipient: PublicUser;
  body: string;
  client_message_id: string;
  created_at: string;
};

export type Conversation = {
  user: PublicUser;
  is_friend: boolean;
  last_message: ChatMessage | null;
};

export type ConversationListResponse = {
  conversations: Conversation[];
};

export type MessagePageResponse = {
  messages: ChatMessage[];
  has_more: boolean;
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

  listMessages: (username: string, beforeID?: number, limit = 30) => {
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    if (beforeID && beforeID > 0) {
      params.set("before_id", String(beforeID));
    }
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
