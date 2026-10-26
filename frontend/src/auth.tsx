import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { Navigate, useLocation } from "react-router-dom";
import {
  api,
  ApiError,
  type BootstrapGroupCursor,
  type BootstrapResponse,
  type RegisterInput,
  type User,
} from "./api";
import {
  clearUser,
  readHints,
  setActiveUser,
} from "./cache";
import type { SyncState } from "./cache";
import { applyBootstrapToCache } from "./workspace";

type AuthStatus = "loading" | "authenticated" | "guest" | "offline";

type AuthContextValue = {
  user: User | null;
  status: AuthStatus;
  notice: string | null;
  bootstrap: BootstrapResponse | null;
  login: (username: string, password: string) => Promise<void>;
  register: (input: RegisterInput) => Promise<void>;
  logout: () => Promise<void>;
  updateUser: (user: User) => void;
  invalidate: (code: string) => void;
  clearNotice: () => void;
};

const AuthContext = createContext<AuthContextValue | null>(null);

const sessionNotices: Record<string, string> = {
  session_replaced: "账号已在其他设备登录，请重新登录。",
  session_expired: "登录已过期，请重新登录。",
  session_password_changed: "密码已修改，请重新登录。",
  session_revoked: "登录已失效，请重新登录。",
};

function groupCursors(sync: SyncState | null): BootstrapGroupCursor[] {
  if (sync === null) {
    return [];
  }
  return Object.entries(sync.groups)
    .map(([groupID, afterID]) => ({
      group_id: Number(groupID),
      after_id: afterID,
    }))
    .filter((cursor) => cursor.group_id > 0 && cursor.after_id > 0)
    .slice(0, 200);
}

async function loadBootstrap(sync: SyncState | null): Promise<BootstrapResponse> {
  return api.bootstrap({
    afterID: sync?.afterID ?? 0,
    groups: groupCursors(sync),
    limit: 200,
  });
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [notice, setNotice] = useState<string | null>(null);
  const [bootstrap, setBootstrap] = useState<BootstrapResponse | null>(null);

  const applyBootstrap = useCallback((payload: BootstrapResponse) => {
    setUser(payload.user);
    setBootstrap(payload);
    setStatus("authenticated");
    setNotice(null);
    void setActiveUser(payload.user);
    void applyBootstrapToCache(payload);
  }, []);

  useEffect(() => {
    let cancelled = false;

    void (async () => {
      const hints = await readHints();
      try {
        const payload = await loadBootstrap(hints.sync);
        if (cancelled) return;
        applyBootstrap(payload);
      } catch (error: unknown) {
        if (cancelled) return;
        if (error instanceof ApiError && error.status === 401) {
          setUser(null);
          setBootstrap(null);
          setStatus("guest");
          if (sessionNotices[error.code]) {
            setNotice(sessionNotices[error.code]);
          }
          return;
        }
        if (hints.user !== null) {
          setUser(hints.user);
          setBootstrap(null);
          setStatus("offline");
          return;
        }
        setUser(null);
        setBootstrap(null);
        setStatus("guest");
        setNotice(
          error instanceof ApiError
            ? error.message
            : "无法连接服务器，请检查网络后重试。",
        );
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [applyBootstrap]);

  const login = useCallback(async (username: string, password: string) => {
    const current = await api.login(username, password);
    setNotice(null);
    try {
      const hints = await readHints();
      const sync = hints.user?.username === current.username ? hints.sync : null;
      applyBootstrap(await loadBootstrap(sync));
    } catch {
      setUser(current);
      setStatus("authenticated");
    }
  }, [applyBootstrap]);

  const register = useCallback(async (input: RegisterInput) => {
    const current = await api.register(input);
    setNotice(null);
    try {
      applyBootstrap(await loadBootstrap(null));
    } catch {
      setUser(current);
      setStatus("authenticated");
    }
  }, [applyBootstrap]);

  const logout = useCallback(async () => {
    const username = user?.username ?? null;
    try {
      await api.logout();
    } finally {
      if (username !== null) {
        await clearUser(username);
      }
      setUser(null);
      setBootstrap(null);
      setStatus("guest");
      setNotice(null);
    }
  }, [user]);

  const updateUser = useCallback((current: User) => {
    setUser(current);
    setBootstrap((previous) => previous === null
      ? previous
      : { ...previous, user: current });
    void setActiveUser(current);
  }, []);

  const invalidate = useCallback((code: string) => {
    setUser(null);
    setBootstrap(null);
    setStatus("guest");
    setNotice(sessionNotices[code] ?? "登录已失效，请重新登录。");
  }, []);

  const clearNotice = useCallback(() => {
    setNotice(null);
  }, []);

  const value = useMemo<AuthContextValue>(() => ({
    user,
    status,
    notice,
    bootstrap,
    login,
    register,
    logout,
    updateUser,
    invalidate,
    clearNotice,
  }), [
    user,
    status,
    notice,
    bootstrap,
    login,
    register,
    logout,
    updateUser,
    invalidate,
    clearNotice,
  ]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const value = useContext(AuthContext);
  if (!value) {
    throw new Error("useAuth must be used inside AuthProvider");
  }
  return value;
}

export function RequireAuth({ children }: { children: ReactNode }) {
  const { status } = useAuth();
  if (status === "loading") {
    return <SessionLoading />;
  }
  if (status === "guest") {
    return <Navigate to="/login" replace />;
  }
  return <>{children}</>;
}

export function GuestOnly({ children }: { children: ReactNode }) {
  const { status } = useAuth();
  const location = useLocation();
  const relogin = new URLSearchParams(location.search).get("relogin") === "1";
  if (status === "loading") {
    return <SessionLoading />;
  }
  if (status === "authenticated" && !relogin) {
    return <Navigate to="/" replace />;
  }
  return <>{children}</>;
}

function SessionLoading() {
  return (
    <main className="page-shell">
      <div className="loading-card" role="status">正在检查登录状态…</div>
    </main>
  );
}
