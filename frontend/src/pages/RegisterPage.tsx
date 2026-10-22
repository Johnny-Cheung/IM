import { useState, type FormEvent } from "react";
import { Link, useNavigate } from "react-router-dom";
import { describeError } from "../api";
import { useAuth } from "../auth";
import { Brand } from "../components/Brand";

const usernamePattern = /^[A-Za-z0-9_]{3,20}$/;

function validate(
  username: string,
  nickname: string,
  password: string,
  confirmation: string,
): string | null {
  if (!usernamePattern.test(username.trim())) {
    return "用户名需为 3–20 位字母、数字或下划线。";
  }
  if ((nickname.trim() === "") || ([...nickname.trim()].length > 20)) {
    return "昵称需为 1–20 个字符。";
  }
  const passwordBytes = new TextEncoder().encode(password).length;
  if ([...password].length < 8 || passwordBytes > 72) {
    return "密码至少 8 个字符，且不超过 72 个字节。";
  }
  if (password !== confirmation) {
    return "两次输入的密码不一致。";
  }
  return null;
}

export function RegisterPage() {
  const { register } = useAuth();
  const navigate = useNavigate();
  const [username, setUsername] = useState("");
  const [nickname, setNickname] = useState("");
  const [password, setPassword] = useState("");
  const [confirmation, setConfirmation] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (submitting) return;

    const validationError = validate(username, nickname, password, confirmation);
    if (validationError) {
      setError(validationError);
      return;
    }

    setError(null);
    setSubmitting(true);
    try {
      await register({
        username: username.trim(),
        nickname: nickname.trim(),
        password,
      });
      navigate("/", { replace: true });
    } catch (submitError) {
      setError(describeError(submitError));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main className="page-shell">
      <section className="auth-card" aria-labelledby="register-title">
        <Brand />
        <p className="eyebrow">创建账号</p>
        <h1 id="register-title">注册</h1>
        <p className="intro">用户名用于登录，昵称用于展示。</p>

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
            <span className="field-hint">3–20 位字母、数字或下划线，注册后不可修改。</span>
          </label>

          <label className="field">
            <span>昵称</span>
            <input
              autoComplete="nickname"
              maxLength={20}
              name="nickname"
              onChange={(event) => setNickname(event.target.value)}
              required
              value={nickname}
            />
          </label>

          <label className="field">
            <span>密码</span>
            <input
              autoComplete="new-password"
              name="password"
              onChange={(event) => setPassword(event.target.value)}
              required
              type="password"
              value={password}
            />
            <span className="field-hint">至少 8 个字符。</span>
          </label>

          <label className="field">
            <span>确认密码</span>
            <input
              autoComplete="new-password"
              name="password-confirmation"
              onChange={(event) => setConfirmation(event.target.value)}
              required
              type="password"
              value={confirmation}
            />
          </label>

          {error && <p className="form-error" role="alert">{error}</p>}

          <button className="button" disabled={submitting} type="submit">
            {submitting ? "注册中…" : "注册"}
          </button>
        </form>

        <p className="switch-line">
          已有账号？<Link to="/login">返回登录</Link>
        </p>
      </section>
    </main>
  );
}
