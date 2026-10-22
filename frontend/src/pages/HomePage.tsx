import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useAuth } from "../auth";
import { Brand } from "../components/Brand";

export function HomePage() {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const [loggingOut, setLoggingOut] = useState(false);

  if (!user) {
    return null;
  }

  const initial = [...user.nickname][0] ?? "I";

  async function handleLogout() {
    if (loggingOut) return;
    setLoggingOut(true);
    try {
      await logout();
    } catch {
      // The local session is cleared even if the network request fails.
    } finally {
      setLoggingOut(false);
    }
    navigate("/login", { replace: true });
  }

  return (
    <main className="page-shell">
      <section className="home-card" aria-labelledby="home-title">
        <header className="home-header">
          <Brand />
          <Link className="text-link" to="/settings">账号设置</Link>
        </header>

        <p className="eyebrow">当前会话</p>
        <h1 id="home-title">已登录</h1>

        <div className="identity">
          <div className="avatar" aria-hidden="true">{initial}</div>
          <div>
            <p className="nickname">{user.nickname}</p>
            <p className="username">@{user.username}</p>
          </div>
        </div>

        <p className="settings-copy">
          好友管理已经可用，聊天会在后续版本加入。
        </p>
        <p className="settings-copy">
          同一账号只保留一个有效登录会话，在其他设备登录后这里会失效。
        </p>

        <div className="button-stack">
          <Link className="button button--secondary" to="/friends">好友管理</Link>
          <button
            className="button button--secondary"
            disabled={loggingOut}
            id="logout-button"
            onClick={() => void handleLogout()}
            type="button"
          >
            {loggingOut ? "退出中…" : "退出登录"}
          </button>
        </div>
      </section>
    </main>
  );
}
