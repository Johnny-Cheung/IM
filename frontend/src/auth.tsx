import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { Navigate } from "react-router-dom";
import {
  api,
  ApiError,
  type RegisterInput,
  type User,
} from "./api";

type AuthStatus = "loading" | "authenticated" | "guest";

type AuthContextValue = {
  user: User | null;
  status: AuthStatus;
  notice: string | null;
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

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    api.me()
      .then((current) => {
        if (cancelled) return;
        setUser(current);
        setStatus("authenticated");
      })
      .catch((error: unknown) => {
        if (cancelled) return;
        setUser(null);
        setStatus("guest");
        if (error instanceof ApiError && sessionNotices[error.code]) {
          setNotice(sessionNotices[error.code]);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const login = useCallback(async (username: string, password: string) => {
    const current = await api.login(username, password);
    setUser(current);
    setStatus("authenticated");
    setNotice(null);
  }, []);

  const register = useCallback(async (input: RegisterInput) => {
    const current = await api.register(input);
    setUser(current);
    setStatus("authenticated");
    setNotice(null);
  }, []);

  const logout = useCallback(async () => {
    try {
      await api.logout();
    } finally {
      setUser(null);
      setStatus("guest");
      setNotice(null);
    }
  }, []);

  const updateUser = useCallback((current: User) => {
    setUser(current);
  }, []);

  const invalidate = useCallback((code: string) => {
    setUser(null);
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
  if (status === "loading") {
    return <SessionLoading />;
  }
  if (status === "authenticated") {
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
