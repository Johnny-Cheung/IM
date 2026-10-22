import { useState, type FormEvent } from "react";
import { Link, useNavigate } from "react-router-dom";
import { describeError } from "../api";
import { useAuth } from "../auth";
import { Brand } from "../components/Brand";

export function LoginPage() {
  const { login, notice } = useAuth();
  const navigate = useNavigate();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (submitting) return;

    setError(null);
    setSubmitting(true);
    try {
      await login(username, password);
      navigate("/", { replace: true });
    } catch (submitError) {
      setError(describeError(submitError));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main className="page-shell">
      <section className="auth-card" aria-labelledby="login-title">
        <Brand />
        <p className="eyebrow">Web 即时通讯</p>
        <h1 id="login-title">登录</h1>
        <p className="intro">使用用户名和密码进入 IM。</p>

        {notice && <p className="notice" role="status">{notice}</p>}

        <form className="form" onSubmit={handleSubmit}>
          <label className="field">
            <span>用户名</span>
            <input
              autoComplete="username"
              autoFocus
              maxLength={20}
              name="username"
              onChange={(event) => setUsername(event.target.value)}
              required
              value={username}
            />
          </label>

          <label className="field">
            <span>密码</span>
            <input
              autoComplete="current-password"
              name="password"
              onChange={(event) => setPassword(event.target.value)}
              required
              type="password"
              value={password}
            />
          </label>

          {error && <p className="form-error" role="alert">{error}</p>}

          <button className="button" disabled={submitting} type="submit">
            {submitting ? "登录中…" : "登录"}
          </button>
        </form>

        <p className="switch-line">
          还没有账号？<Link to="/register">注册</Link>
        </p>
      </section>
    </main>
  );
}
