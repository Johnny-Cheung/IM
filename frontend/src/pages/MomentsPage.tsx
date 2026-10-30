import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
} from "react";
import { Link, useNavigate } from "react-router-dom";
import {
  api,
  describeError,
  isSessionError,
  type Moment,
  type MomentDetailResponse,
  type MomentVisibility,
} from "../api";
import { useAuth } from "../auth";
import { Brand } from "../components/Brand";
import {
  COMMENT_LIMIT,
  MOMENT_BODY_LIMIT,
  formatMomentTime,
  initialOf,
  mergeMomentComments,
  runeCount,
} from "../moments";

const MOMENT_LIMIT = 20;
const COMMENT_PAGE_SIZE = 20;

export function MomentsPage() {
  const { user, invalidate } = useAuth();
  const navigate = useNavigate();

  const [moments, setMoments] = useState<Moment[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [nextBefore, setNextBefore] = useState(0);

  const [body, setBody] = useState("");
  const [visibility, setVisibility] = useState<MomentVisibility>("friends");
  const [posting, setPosting] = useState(false);

  const [expandedID, setExpandedID] = useState<number | null>(null);
  const [details, setDetails] = useState<Record<number, MomentDetailResponse>>({});
  const [detailLoading, setDetailLoading] = useState<number | null>(null);
  const [commentDrafts, setCommentDrafts] = useState<Record<number, string>>({});
  const [busy, setBusy] = useState<string | null>(null);

  const mountedRef = useRef(true);
  const seenWatermarkRef = useRef(0);
  const detailVersionsRef = useRef<Record<number, number>>({});
  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const advanceSeen = useCallback((items: readonly Moment[]) => {
    const newestFriend = items.reduce(
      (max, moment) => (!moment.mine && moment.visibility === "friends"
        ? Math.max(max, moment.id)
        : max),
      0,
    );
    if (newestFriend > seenWatermarkRef.current) {
      seenWatermarkRef.current = newestFriend;
      void api.markMomentsSeen(newestFriend).catch(() => {});
    }
  }, []);

  const touchDetail = useCallback((momentID: number) => {
    detailVersionsRef.current[momentID] = (detailVersionsRef.current[momentID] ?? 0) + 1;
  }, []);

  const handleSessionError = useCallback((error: unknown): boolean => {
    if (!isSessionError(error)) {
      return false;
    }
    invalidate(error.code);
    navigate("/login", { replace: true });
    return true;
  }, [invalidate, navigate]);

  const loadFirst = useCallback(async () => {
    setLoading(true);
    setLoadError(null);
    try {
      const page = await api.listMoments(0, MOMENT_LIMIT);
      if (!mountedRef.current) {
        return;
      }
      setMoments(page.moments);
      setHasMore(page.has_more);
      setNextBefore(page.next_before_id);
      setDetails({});
      setExpandedID(null);
      setCommentDrafts({});
      advanceSeen(page.moments);
    } catch (error) {
      if (!handleSessionError(error)) {
        setLoadError(describeError(error));
      }
    } finally {
      if (mountedRef.current) {
        setLoading(false);
      }
    }
  }, [advanceSeen, handleSessionError]);

  useEffect(() => {
    void loadFirst();
  }, [loadFirst]);
  const loadMore = useCallback(async () => {
    if (loadingMore || !hasMore || nextBefore <= 0) {
      return;
    }
    setLoadingMore(true);
    setActionError(null);
    try {
      const page = await api.listMoments(nextBefore, MOMENT_LIMIT);
      if (!mountedRef.current) {
        return;
      }
      setMoments((current) => {
        const seen = new Set(current.map((moment) => moment.id));
        return [...current, ...page.moments.filter((moment) => !seen.has(moment.id))];
      });
      setHasMore(page.has_more);
      setNextBefore(page.next_before_id);
      advanceSeen(page.moments);
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      if (mountedRef.current) {
        setLoadingMore(false);
      }
    }
  }, [advanceSeen, hasMore, handleSessionError, loadingMore, nextBefore]);

  const updateMoment = useCallback((momentID: number, update: (moment: Moment) => Moment) => {
    setMoments((current) => current.map((moment) =>
      moment.id === momentID ? update(moment) : moment));
    setDetails((current) => {
      const detail = current[momentID];
      if (detail === undefined) {
        return current;
      }
      return {
        ...current,
        [momentID]: { ...detail, moment: update(detail.moment) },
      };
    });
  }, []);

  const handlePost = async (event: FormEvent) => {
    event.preventDefault();
    if (posting) {
      return;
    }
    const text = body.trim();
    if (text === "") {
      setActionError("写点什么再发布。");
      return;
    }
    if (runeCount(text) > MOMENT_BODY_LIMIT) {
      setActionError(`动态最多 ${MOMENT_BODY_LIMIT} 字。`);
      return;
    }
    setPosting(true);
    setActionError(null);
    try {
      const moment = await api.createMoment(text, visibility);
      if (!mountedRef.current) {
        return;
      }
      setMoments((current) => [moment, ...current.filter((item) => item.id !== moment.id)]);
      setBody("");
      setVisibility("friends");
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      if (mountedRef.current) {
        setPosting(false);
      }
    }
  };

  const loadDetail = useCallback(async (momentID: number) => {
    const version = detailVersionsRef.current[momentID] ?? 0;
    setDetailLoading(momentID);
    setActionError(null);
    try {
      const detail = await api.getMoment(momentID);
      if (!mountedRef.current
        || version !== (detailVersionsRef.current[momentID] ?? 0)) {
        return;
      }
      setDetails((current) => ({ ...current, [momentID]: detail }));
      setMoments((current) => current.map((moment) => {
        if (moment.id !== momentID) {
          return moment;
        }
        return {
          ...moment,
          like_count: detail.moment.like_count,
          comment_count: detail.moment.comment_count,
          liked_by_me: detail.moment.liked_by_me,
          comments_preview: detail.comments.slice(-2),
        };
      }));
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      if (mountedRef.current) {
        setDetailLoading(null);
      }
    }
  }, [handleSessionError]);

  const toggleExpanded = (momentID: number) => {
    if (expandedID === momentID) {
      setExpandedID(null);
      return;
    }
    setExpandedID(momentID);
    void loadDetail(momentID);
  };

  const toggleLike = async (moment: Moment) => {
    const key = `like-${moment.id}`;
    if (busy === key) {
      return;
    }
    setBusy(key);
    setActionError(null);
    try {
      const result = moment.liked_by_me
        ? await api.unlikeMoment(moment.id)
        : await api.likeMoment(moment.id);
      if (!mountedRef.current) {
        return;
      }
      touchDetail(moment.id);
      updateMoment(moment.id, (item) => ({
        ...item,
        like_count: result.like_count,
        liked_by_me: result.liked_by_me,
      }));
      setDetails((current) => {
        const detail = current[moment.id];
        if (detail === undefined) {
          return current;
        }
        const liked = result.liked_by_me;
        const likers = liked
          ? (detail.likers.some((like) => like.user.username === user?.username)
            ? detail.likers
            : [...detail.likers, {
              id: -1,
              user: { username: user?.username ?? "", nickname: user?.nickname ?? "" },
              created_at: new Date().toISOString(),
            }])
          : detail.likers.filter((like) => like.user.username !== user?.username);
        return {
          ...current,
          [moment.id]: {
            ...detail,
            moment: {
              ...detail.moment,
              like_count: result.like_count,
              liked_by_me: result.liked_by_me,
            },
            likers,
          },
        };
      });
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      if (mountedRef.current) {
        setBusy(null);
      }
    }
  };

  const submitComment = async (momentID: number) => {
    const draft = (commentDrafts[momentID] ?? "").trim();
    if (draft === "") {
      return;
    }
    if (runeCount(draft) > COMMENT_LIMIT) {
      setActionError(`评论最多 ${COMMENT_LIMIT} 字。`);
      return;
    }
    const key = `comment-${momentID}`;
    if (busy === key) {
      return;
    }
    setBusy(key);
    setActionError(null);
    try {
      const comment = await api.createMomentComment(momentID, draft);
      if (!mountedRef.current) {
        return;
      }
      touchDetail(momentID);
      setCommentDrafts((current) => ({ ...current, [momentID]: "" }));
      setMoments((current) => current.map((moment) => {
        if (moment.id !== momentID) {
          return moment;
        }
        const preview = [...moment.comments_preview, comment].slice(-2);
        return { ...moment, comment_count: moment.comment_count + 1, comments_preview: preview };
      }));
      setDetails((current) => {
        const detail = current[momentID];
        if (detail === undefined) {
          return current;
        }
        return {
          ...current,
          [momentID]: {
            ...detail,
            moment: { ...detail.moment, comment_count: detail.moment.comment_count + 1 },
            comments: mergeMomentComments(detail.comments, [comment]),
          },
        };
      });
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      if (mountedRef.current) {
        setBusy(null);
      }
    }
  };

  const removeComment = async (momentID: number, commentID: number) => {
    if (!window.confirm("删除这条评论？")) {
      return;
    }
    const key = `comment-delete-${commentID}`;
    if (busy === key) {
      return;
    }
    setBusy(key);
    setActionError(null);
    try {
      await api.deleteMomentComment(momentID, commentID);
      if (!mountedRef.current) {
        return;
      }
      touchDetail(momentID);
      setMoments((current) => current.map((moment) => {
        if (moment.id !== momentID) {
          return moment;
        }
        return {
          ...moment,
          comment_count: Math.max(0, moment.comment_count - 1),
          comments_preview: moment.comments_preview.filter((comment) => comment.id !== commentID),
        };
      }));
      setDetails((current) => {
        const detail = current[momentID];
        if (detail === undefined) {
          return current;
        }
        return {
          ...current,
          [momentID]: {
            ...detail,
            moment: {
              ...detail.moment,
              comment_count: Math.max(0, detail.moment.comment_count - 1),
            },
            comments: detail.comments.filter((comment) => comment.id !== commentID),
          },
        };
      });
      void loadDetail(momentID);
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      if (mountedRef.current) {
        setBusy(null);
      }
    }
  };

  const loadMoreComments = async (momentID: number) => {
    const detail = details[momentID];
    if (detail === undefined || detail.comments.length === 0) {
      return;
    }
    const key = `comments-more-${momentID}`;
    if (busy === key) {
      return;
    }
    setBusy(key);
    setActionError(null);
    try {
      const page = await api.listMomentComments(
        momentID,
        detail.comments[0].id,
        COMMENT_PAGE_SIZE,
      );
      if (!mountedRef.current) {
        return;
      }
      setDetails((current) => {
        const latest = current[momentID];
        if (latest === undefined) {
          return current;
        }
        return {
          ...current,
          [momentID]: {
            ...latest,
            comments: mergeMomentComments(page.comments, latest.comments),
            comments_has_more: page.has_more,
          },
        };
      });
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      if (mountedRef.current) {
        setBusy(null);
      }
    }
  };

  const removeMoment = async (momentID: number) => {
    if (!window.confirm("删除这条动态？删除后不可恢复。")) {
      return;
    }
    const key = `delete-${momentID}`;
    if (busy === key) {
      return;
    }
    setBusy(key);
    setActionError(null);
    try {
      await api.deleteMoment(momentID);
      if (!mountedRef.current) {
        return;
      }
      setMoments((current) => current.filter((moment) => moment.id !== momentID));
      setDetails((current) => {
        const next = { ...current };
        delete next[momentID];
        return next;
      });
      setExpandedID((current) => (current === momentID ? null : current));
    } catch (error) {
      if (!handleSessionError(error)) {
        setActionError(describeError(error));
      }
    } finally {
      if (mountedRef.current) {
        setBusy(null);
      }
    }
  };

  const bodyCount = useMemo(() => runeCount(body), [body]);
  if (user === null) {
    return null;
  }

  return (
    <main className="page-shell">
      <section className="moments-card">
        <header className="moments-header">
          <div className="brand-row">
            <Brand />
            <div>
              <p className="eyebrow">Moments</p>
              <h1>朋友圈</h1>
            </div>
          </div>
          <nav className="header-links" aria-label="主导航">
            <Link className="text-link" to="/">返回聊天</Link>
            <Link className="text-link" to="/friends">好友管理</Link>
            <Link className="text-link" to="/settings">账号设置</Link>
          </nav>
        </header>

        <form className="moments-composer" onSubmit={handlePost}>
          <textarea
            aria-label="写动态"
            disabled={posting}
            onChange={(event) => setBody(event.target.value)}
            placeholder={`分享点什么，${user.nickname}…`}
            rows={3}
            value={body}
          />
          <div className="moments-composer-actions">
            <label className="moments-visibility">
              <span>可见范围</span>
              <select
                disabled={posting}
                onChange={(event) => setVisibility(event.target.value as MomentVisibility)}
                value={visibility}
              >
                <option value="friends">好友可见</option>
                <option value="private">仅自己可见</option>
              </select>
            </label>
            <span className={bodyCount > MOMENT_BODY_LIMIT ? "composer-count composer-count--over" : "composer-count"}>
              {bodyCount}/{MOMENT_BODY_LIMIT}
            </span>
            <button
              className="button button--compact"
              disabled={posting || bodyCount === 0 || bodyCount > MOMENT_BODY_LIMIT}
              type="submit"
            >
              {posting ? "发布中…" : "发布"}
            </button>
          </div>
        </form>

        {actionError !== null && (
          <p className="form-error" role="alert">{actionError}</p>
        )}

        {loading ? (
          <p className="empty-state">正在加载朋友圈…</p>
        ) : loadError !== null ? (
          <div className="form-error" role="alert">
            <p>{loadError}</p>
            <button className="button button--compact button--secondary" onClick={() => void loadFirst()} type="button">
              重试
            </button>
          </div>
        ) : moments.length === 0 ? (
          <p className="empty-state">还没有动态，发布第一条吧。</p>
        ) : (
          <div className="moments-list">
            {moments.map((moment) => {
              const detail = details[moment.id];
              const expanded = expandedID === moment.id;
              const likeBusy = busy === `like-${moment.id}`;
              const commentBusy = busy === `comment-${moment.id}`;
              return (
                <article className="moment-card" key={moment.id}>
                  <header className="moment-head">
                    <div className="moment-author">
                      <span className="moment-avatar" aria-hidden="true">
                        {initialOf(moment.author.nickname)}
                      </span>
                      <div>
                        <strong>{moment.author.nickname}</strong>
                        <span className="moment-username">@{moment.author.username}</span>
                        <span className="moment-time">{formatMomentTime(moment.created_at)}</span>
                        {moment.visibility === "private" && (
                          <span className="moment-private">仅自己可见</span>
                        )}
                      </div>
                    </div>
                    {moment.mine && (
                      <button
                        className="text-link text-link--button text-link--danger"
                        disabled={busy === `delete-${moment.id}`}
                        onClick={() => void removeMoment(moment.id)}
                        type="button"
                      >
                        删除
                      </button>
                    )}
                  </header>

                  <p className="moment-body">{moment.body}</p>

                  <div className="moment-actions">
                    <button
                      className={moment.liked_by_me ? "moment-action moment-action--active" : "moment-action"}
                      disabled={likeBusy}
                      onClick={() => void toggleLike(moment)}
                      type="button"
                    >
                      {moment.liked_by_me ? "取消赞" : "赞"}
                      {moment.like_count > 0 ? ` ${moment.like_count}` : ""}
                    </button>
                    <button
                      className="moment-action"
                      onClick={() => toggleExpanded(moment.id)}
                      type="button"
                    >
                      评论{moment.comment_count > 0 ? ` ${moment.comment_count}` : ""}
                    </button>
                  </div>

                  {!expanded && moment.comments_preview.length > 0 && (
                    <ul className="moment-comments">
                      {moment.comments_preview.map((comment) => (
                        <li key={comment.id}>
                          <strong>{comment.author.nickname}</strong>：{comment.body}
                        </li>
                      ))}
                    </ul>
                  )}

                  {expanded && (
                    <section className="moment-detail">
                      {detailLoading === moment.id && detail === undefined ? (
                        <p className="empty-state">正在加载评论…</p>
                      ) : detail === undefined ? null : (
                        <>
                          {detail.likers.length > 0 && (
                            <p className="moment-likers">
                              赞：{detail.likers.map((like) => like.user.nickname).join("、")}
                            </p>
                          )}
                          <ul className="moment-comments">
                            {detail.comments.map((comment) => (
                              <li key={comment.id}>
                                <strong>{comment.author.nickname}</strong>：{comment.body}
                                {(moment.mine || comment.author.username === user.username) && (
                                  <button
                                    className="text-link text-link--button text-link--danger moment-comment-delete"
                                    disabled={busy === `comment-delete-${comment.id}`}
                                    onClick={() => void removeComment(moment.id, comment.id)}
                                    type="button"
                                  >
                                    删除
                                  </button>
                                )}
                              </li>
                            ))}
                            {detail.comments.length === 0 && (
                              <li className="moment-empty">还没有评论。</li>
                            )}
                          </ul>
                          {detail.comments_has_more && (
                            <button
                              className="text-link text-link--button"
                              disabled={busy === `comments-more-${moment.id}`}
                              onClick={() => void loadMoreComments(moment.id)}
                              type="button"
                            >
                              加载更早的评论
                            </button>
                          )}
                          <form
                            className="moment-comment-form"
                            onSubmit={(event) => {
                              event.preventDefault();
                              void submitComment(moment.id);
                            }}
                          >
                            <input
                              aria-label="写评论"
                              disabled={commentBusy}
                              maxLength={COMMENT_LIMIT}
                              onChange={(event) => setCommentDrafts((current) => ({
                                ...current,
                                [moment.id]: event.target.value,
                              }))}
                              placeholder="写评论…"
                              value={commentDrafts[moment.id] ?? ""}
                            />
                            <button
                              className="button button--compact"
                              disabled={commentBusy || (commentDrafts[moment.id] ?? "").trim() === ""}
                              type="submit"
                            >
                              {commentBusy ? "发送中…" : "发送"}
                            </button>
                          </form>
                        </>
                      )}
                    </section>
                  )}
                </article>
              );
            })}
          </div>
        )}

        {hasMore && !loading && (
          <button
            className="button button--secondary"
            disabled={loadingMore}
            onClick={() => void loadMore()}
            type="button"
          >
            {loadingMore ? "加载中…" : "加载更多"}
          </button>
        )}
      </section>
    </main>
  );
}
