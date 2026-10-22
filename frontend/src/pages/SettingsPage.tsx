import { useEffect, useState, type FormEvent } from "react";
import { Link, useNavigate } from "react-router-dom";
import { api, describeError, isSessionError } from "../api";
import { useAuth } from "../auth";
import { Brand } from "../components/Brand";

export function SettingsPage() {
  const { user, updateUser, invalidate } = useAuth();
  const navigate = useNavigate();

  const [nickname, setNickname] = useState(user?.nickname ?? "");
  const [nicknameError, setNicknameError] = useState<string | null>(null);
  const [nicknameSuccess, setNicknameSuccess] = useState(false);
  const [savingNickname, setSavingNickname] = useState(false);

  const [oldPassword, setOldPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmation, setConfirmation] = useState("");
  const [passwordError, setPasswordError] = useState<string | null>(null);
  const [savingPassword, setSavingPassword] = useState(false);

  useEffect(() => {
    if (user) {
      setNickname(user.nickname);
    }
  }, [user]);

  if (!user) {
    return null;
  }

  function handleSessionFailure(error: unknown): boolean {
    if (!isSessionError(error)) {
      return false;
    }
    invalidate(error.code);
    navigate("/login", { replace: true });
    return true;
  }

  async function handleNicknameSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (savingNickname) return;

    setNicknameError(null);
    setNicknameSuccess(false);
    const trimmed = nickname.trim();
    if (trimmed === "" || [...trimmed].length > 20) {
      setNicknameError("昵称需为 1–20 个字符。");
      return;
    }

    setSavingNickname(true);
    try {
      const updated = await api.updateNickname(trimmed);
      updateUser(updated);
      setNickname(updated.nickname);
      setNicknameSuccess(true);
    } catch (error) {
      if (!handleSessionFailure(error)) {
        setNicknameError(describeError(error));
      }
    } finally {
      setSavingNickname(false);
    }
  }

  async function handlePasswordSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (savingPassword) return;

    setPasswordError(null);
    const passwordBytes = new TextEncoder().encode(newPassword).length;
    if ([...newPassword].length < 8 || passwordBytes > 72) {
      setPasswordError("新密码至少 8 个字符，且不超过 72 个字节。");
      return;
    }
    if (newPassword !== confirmation) {
      setPasswordError("两次输入的新密码不一致。");
      return;
    }

    setSavingPassword(true);
    try {
      await api.updatePassword(oldPassword, newPassword);
      invalidate("session_password_changed");
      navigate("/login", { replace: true });
    } catch (error) {
      if (!handleSessionFailure(error)) {
        setPasswordError(describeError(error));
      }
    } finally {
      setSavingPassword(false);
    }
  }

  return (
    <main className="page-shell">
      <section className="settings-card" aria-labelledby="settings-title">
        <header className="settings-header">
          <Brand />
          <Link className="text-link" to="/">返回主页</Link>
        </header>

        <p className="eyebrow">账号管理</p>
        <h1 id="settings-title">账号设置</h1>
        <p className="intro">修改昵称或密码。用户名不可修改。</p>

        <section className="settings-section" aria-labelledby="account-heading">
          <h2 id="account-heading">账号信息</h2>
          <label className="field">
            <span>用户名</span>
            <input readOnly value={user.username} />
            <span className="field-hint">用户名是登录名和对外 ID，注册后不可修改。</span>
          </label>
        </section>

        <section className="settings-section" aria-labelledby="nickname-heading">
          <h2 id="nickname-heading">昵称</h2>
          <p className="settings-copy">昵称用于展示，可以重复。</p>
          <form className="form" onSubmit={handleNicknameSubmit}>
            <label className="field">
              <span>新昵称</span>
              <input
                autoComplete="nickname"
                maxLength={20}
                onChange={(event) => {
                  setNickname(event.target.value);
                  setNicknameSuccess(false);
                }}
                required
                value={nickname}
              />
            </label>

            {nicknameError && <p className="form-error" role="alert">{nicknameError}</p>}
            {nicknameSuccess && <p className="success" role="status">昵称已更新。</p>}

            <button className="button" disabled={savingNickname} type="submit">
              {savingNickname ? "保存中…" : "保存昵称"}
            </button>
          </form>
        </section>

        <section className="settings-section" aria-labelledby="password-heading">
          <h2 id="password-heading">修改密码</h2>
          <p className="settings-copy">
            修改成功后，本账号的全部登录会话都会失效，需要重新登录。
          </p>
          <form className="form" onSubmit={handlePasswordSubmit}>
            <label className="field">
              <span>旧密码</span>
              <input
                autoComplete="current-password"
                onChange={(event) => setOldPassword(event.target.value)}
                required
                type="password"
                value={oldPassword}
              />
            </label>

            <label className="field">
              <span>新密码</span>
              <input
                autoComplete="new-password"
                onChange={(event) => setNewPassword(event.target.value)}
                required
                type="password"
                value={newPassword}
              />
              <span className="field-hint">至少 8 个字符。</span>
            </label>

            <label className="field">
              <span>确认新密码</span>
              <input
                autoComplete="new-password"
                onChange={(event) => setConfirmation(event.target.value)}
                required
                type="password"
                value={confirmation}
              />
            </label>

            {passwordError && <p className="form-error" role="alert">{passwordError}</p>}

            <button className="button" disabled={savingPassword} type="submit">
              {savingPassword ? "修改中…" : "修改密码"}
            </button>
          </form>
        </section>
      </section>
    </main>
  );
}
