export type User = {
  username: string;
  nickname: string;
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
};

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
