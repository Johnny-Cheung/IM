import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type FormEvent,
} from "react";
import { Link, useNavigate } from "react-router-dom";
import {
  api,
  describeError,
  isSessionError,
  type FriendRequest,
  type FriendRequestStatus,
  type FriendRequests,
  type PublicUser,
  type Relationship,
  type UserLookup,
} from "../api";
import { useAuth } from "../auth";
import { Brand } from "../components/Brand";

const relationshipLabels: Record<Relationship, string> = {
  self: "这是你自己",
  friend: "已是好友",
  incoming_pending: "对方已申请，请在下方处理",
  outgoing_pending: "已申请，等待对方处理",
  none: "可以添加好友",
};

const historyStatusLabels: Record<FriendRequestStatus, string> = {
  pending: "未处理",
  accepted: "已通过",
  rejected: "已拒绝",
};

const dateTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

function formatDateTime(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : dateTimeFormatter.format(date);
}

function initialOf(name: string): string {
  return [...name][0] ?? "?";
}

export function FriendsPage() {
  const { invalidate } = useAuth();
  const navigate = useNavigate();

  const [friends, setFriends] = useState<PublicUser[]>([]);
  const [requests, setRequests] = useState<FriendRequests>({
    incoming: [],
    outgoing: [],
    history: [],
  });
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [searchTerm, setSearchTerm] = useState("");
  const [searchResult, setSearchResult] = useState<UserLookup | null>(null);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [searching, setSearching] = useState(false);

  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [confirmingDelete, setConfirmingDelete] = useState<string | null>(null);

  const refreshingRef = useRef(false);

  const handleSessionError = useCallback((error: unknown): boolean => {
    if (!isSessionError(error)) {
      return false;
    }
    invalidate(error.code);
    navigate("/login", { replace: true });
    return true;
  }, [invalidate, navigate]);

  const refresh = useCallback(async (initial = false) => {
    if (refreshingRef.current) {
      return;
    }
    refreshingRef.current = true;
    setRefreshing(true);
    if (initial) {
      setLoading(true);
    }

    try {
      const [friendList, requestList] = await Promise.all([
        api.listFriends(),
        api.listFriendRequests(),
      ]);
      setFriends(friendList.friends);
      setRequests(requestList);
      setLoadError(null);
    } catch (error) {
      if (!handleSessionError(error)) {
        setLoadError(describeError(error));
      }
    } finally {
      refreshingRef.current = false;
      setRefreshing(false);
      setLoading(false);
    }
  }, [handleSessionError]);

  useEffect(() => {
    void refresh(true);
  }, [refresh]);

  useEffect(() => {
    const refreshWhenVisible = () => {
      if (document.visibilityState === "visible") {
        void refresh(false);
      }
    };
    document.addEventListener("visibilitychange", refreshWhenVisible);
    window.addEventListener("focus", refreshWhenVisible);
    return () => {
      document.removeEventListener("visibilitychange", refreshWhenVisible);
      window.removeEventListener("focus", refreshWhenVisible);
    };
  }, [refresh]);

  async function handleSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const username = searchTerm.trim();
    if (username === "") {
      setSearchResult(null);
      setSearchError("请输入完整用户名。");
      return;
    }

    setSearching(true);
    setSearchError(null);
    setActionError(null);
    setActionMessage(null);
    try {
      setSearchResult(await api.lookupUser(username));
    } catch (error) {
      if (!handleSessionError(error)) {
        setSearchResult(null);
        setSearchError(describeError(error));
      }
    } finally {
      setSearching(false);
    }
  }

  async function sendRequest(username: string) {
    setActionError(null);
    setActionMessage(null);
    setBusyAction(`add:${username}`);
    try {
      const result = await api.sendFriendRequest(username);
      if (result.status === "accepted") {
        setActionMessage(`你和 ${result.friend?.nickname ?? username} 已成为好友。`);
        setSearchResult((previous) => previous
          ? { ...previous, relationship: "friend" }
          : previous);
      } else {
        setActionMessage("好友申请已发送。");
        setSearchResult((previous) => previous
          ? { ...previous, relationship: "outgoing_pending" }
          : previous);
      }
      await refresh(false);
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      setBusyAction(null);
    }
  }

  async function acceptRequest(request: FriendRequest) {
    setActionError(null);
    setActionMessage(null);
    setBusyAction(`accept:${request.id}`);
    try {
      await api.acceptFriendRequest(request.id);
      setActionMessage(`已接受 ${request.user.nickname} 的好友申请。`);
      setSearchResult((previous) => previous && previous.username === request.user.username
        ? { ...previous, relationship: "friend" }
        : previous);
      await refresh(false);
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      setBusyAction(null);
    }
  }

  async function rejectRequest(request: FriendRequest) {
    setActionError(null);
    setActionMessage(null);
    setBusyAction(`reject:${request.id}`);
    try {
      await api.rejectFriendRequest(request.id);
      setActionMessage(`已拒绝 ${request.user.nickname} 的好友申请。`);
      setSearchResult((previous) => previous && previous.username === request.user.username
        ? { ...previous, relationship: "none" }
        : previous);
      await refresh(false);
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      setBusyAction(null);
    }
  }

  async function deleteFriend(friend: PublicUser) {
    setActionError(null);
    setActionMessage(null);
    setBusyAction(`delete:${friend.username}`);
    try {
      await api.deleteFriend(friend.username);
      setConfirmingDelete(null);
      setActionMessage(`已删除好友 ${friend.nickname}。`);
      setSearchResult((previous) => previous && previous.username === friend.username
        ? { ...previous, relationship: "none" }
        : previous);
      await refresh(false);
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      setBusyAction(null);
    }
  }

  return (
    <main className="page-shell">
      <section className="friends-card" aria-labelledby="friends-title">
        <header className="friends-header">
          <Brand />
          <div className="header-links">
            <Link className="text-link" to="/">返回主页</Link>
            <Link className="text-link" to="/settings">账号设置</Link>
          </div>
        </header>

        <div className="friends-title-row">
          <div>
            <p className="eyebrow">好友管理</p>
            <h1 id="friends-title">好友</h1>
          </div>
          <button
            className="button button--compact button--secondary"
            disabled={refreshing}
            id="refresh-friends"
            onClick={() => void refresh(false)}
            type="button"
          >
            {refreshing ? "刷新中…" : "刷新"}
          </button>
        </div>
        <p className="intro">通过完整用户名精确查找，管理好友申请和好友列表。</p>

        {actionMessage && <p className="success" role="status">{actionMessage}</p>}
        {actionError && <p className="form-error" role="alert">{actionError}</p>}

        {loading ? (
          <div className="loading-card" role="status">正在加载好友…</div>
        ) : loadError ? (
          <div className="friends-error">
            <p className="form-error" role="alert">{loadError}</p>
            <button
              className="button button--compact"
              onClick={() => void refresh(true)}
              type="button"
            >
              重试
            </button>
          </div>
        ) : (
          <>
            <section className="friends-section" aria-labelledby="add-friend-heading">
              <h2 id="add-friend-heading">添加好友</h2>
              <p className="settings-copy">输入完整用户名，不区分大小写。</p>
              <form className="search-form" id="friend-search-form" onSubmit={handleSearch}>
                <label className="sr-only" htmlFor="friend-username">用户名</label>
                <input
                  id="friend-username"
                  maxLength={20}
                  onChange={(event) => setSearchTerm(event.target.value)}
                  placeholder="输入用户名"
                  value={searchTerm}
                />
                <button className="button button--compact" disabled={searching} type="submit">
                  {searching ? "查找中…" : "查找"}
                </button>
              </form>

              {searchError && <p className="form-error" role="alert">{searchError}</p>}

              {searchResult && (
                <div className="search-result">
                  <div className="person">
                    <div className="avatar avatar--small" aria-hidden="true">
                      {initialOf(searchResult.nickname)}
                    </div>
                    <div className="person-info">
                      <p className="person-name">{searchResult.nickname}</p>
                      <p className="person-username">@{searchResult.username}</p>
                    </div>
                    <div className="person-actions">
                      {searchResult.relationship === "none" ? (
                        <button
                          className="button button--compact"
                          disabled={busyAction === `add:${searchResult.username}`}
                          onClick={() => void sendRequest(searchResult.username)}
                          type="button"
                        >
                          {busyAction === `add:${searchResult.username}` ? "发送中…" : "添加好友"}
                        </button>
                      ) : (
                        <span className="relationship-label">
                          {relationshipLabels[searchResult.relationship]}
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </section>

            <section className="friends-section" id="friend-requests" aria-labelledby="requests-heading">
              <div className="section-heading">
                <h2 id="requests-heading">好友申请</h2>
                <span className="count-badge">{requests.incoming.length}</span>
              </div>

              {requests.incoming.length === 0 ? (
                <p className="empty-state">暂无收到的好友申请。</p>
              ) : (
                <ul className="people-list">
                  {requests.incoming.map((request) => (
                    <li className="person" data-request-id={request.id} key={request.id}>
                      <div className="avatar avatar--small" aria-hidden="true">
                        {initialOf(request.user.nickname)}
                      </div>
                      <div className="person-info">
                        <p className="person-name">{request.user.nickname}</p>
                        <p className="person-username">@{request.user.username}</p>
                      </div>
                      <div className="person-actions">
                        <button
                          className="button button--compact"
                          disabled={busyAction === `accept:${request.id}`}
                          onClick={() => void acceptRequest(request)}
                          type="button"
                        >
                          {busyAction === `accept:${request.id}` ? "接受中…" : "接受"}
                        </button>
                        <button
                          className="button button--compact button--danger"
                          disabled={busyAction === `reject:${request.id}`}
                          onClick={() => void rejectRequest(request)}
                          type="button"
                        >
                          {busyAction === `reject:${request.id}` ? "拒绝中…" : "拒绝"}
                        </button>
                      </div>
                    </li>
                  ))}
                </ul>
              )}

              {requests.outgoing.length > 0 && (
                <div className="outgoing-block">
                  <h3>我发出的申请</h3>
                  <ul className="people-list">
                    {requests.outgoing.map((request) => (
                      <li className="person" data-username={request.user.username} key={request.id}>
                        <div className="avatar avatar--small" aria-hidden="true">
                          {initialOf(request.user.nickname)}
                        </div>
                        <div className="person-info">
                          <p className="person-name">{request.user.nickname}</p>
                          <p className="person-username">@{request.user.username}</p>
                        </div>
                        <span className="relationship-label">等待对方处理</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </section>

            <section className="friends-section" id="friends-list" aria-labelledby="friends-list-heading">
              <div className="section-heading">
                <h2 id="friends-list-heading">我的好友</h2>
                <span className="count-badge">{friends.length}</span>
              </div>

              {friends.length === 0 ? (
                <p className="empty-state">还没有好友，先通过用户名添加一个。</p>
              ) : (
                <ul className="people-list">
                  {friends.map((friend) => (
                    <li className="person" data-username={friend.username} key={friend.username}>
                      <div className="avatar avatar--small" aria-hidden="true">
                        {initialOf(friend.nickname)}
                      </div>
                      <div className="person-info">
                        <p className="person-name">{friend.nickname}</p>
                        <p className="person-username">@{friend.username}</p>
                      </div>
                      <div className="person-actions">
                        {confirmingDelete === friend.username ? (
                          <>
                            <button
                              className="button button--compact button--danger"
                              disabled={busyAction === `delete:${friend.username}`}
                              onClick={() => void deleteFriend(friend)}
                              type="button"
                            >
                              {busyAction === `delete:${friend.username}` ? "删除中…" : "确认删除"}
                            </button>
                            <button
                              className="button button--compact button--secondary"
                              onClick={() => setConfirmingDelete(null)}
                              type="button"
                            >
                              取消
                            </button>
                          </>
                        ) : (
                          <>
                            <Link
                              className="button button--compact button--secondary"
                              to={`/chat/${encodeURIComponent(friend.username)}`}
                            >
                              发消息
                            </Link>
                            <button
                              className="button button--compact button--secondary"
                              onClick={() => setConfirmingDelete(friend.username)}
                              type="button"
                            >
                              删除
                            </button>
                          </>
                        )}
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </section>

            <section
              className="friends-section"
              id="friend-request-history"
              aria-labelledby="history-heading"
            >
              <div className="section-heading">
                <h2 id="history-heading">好友记录</h2>
                <span className="count-badge">{requests.history.length}</span>
              </div>

              {requests.history.length === 0 ? (
                <p className="empty-state">还没有好友记录。</p>
              ) : (
                <ul className="people-list">
                  {requests.history.map((record) => (
                    <li className="person" data-request-id={record.id} key={record.id}>
                      <div className="avatar avatar--small" aria-hidden="true">
                        {initialOf(record.user.nickname)}
                      </div>
                      <div className="person-info">
                        <p className="person-name">{record.user.nickname}</p>
                        <p className="person-username">@{record.user.username}</p>
                        <p className="history-meta">
                          {record.direction === "outgoing" ? "我发出" : "收到"} ·{" "}
                          <time dateTime={record.created_at}>
                            {formatDateTime(record.created_at)}
                          </time>
                        </p>
                      </div>
                      <span
                        className={`status-chip status-chip--${record.status}`}
                        title={record.handled_at
                          ? `处理时间：${formatDateTime(record.handled_at)}`
                          : undefined}
                      >
                        {historyStatusLabels[record.status]}
                      </span>
                    </li>
                  ))}
                </ul>
              )}
            </section>
          </>
        )}
      </section>
    </main>
  );
}
