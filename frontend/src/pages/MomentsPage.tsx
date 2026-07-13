import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { ArrowUp, ChevronDown, CircleAlert, Heart, LoaderCircle, MessageCircle, PenLine, Trash2, Users, UsersRound } from "lucide-react";
import { useState } from "react";
import { ApiError } from "../api/client";
import { Avatar, Button, ConfirmDialog, Drawer } from "../components/ui";
import type { MomentLiker } from "../../goim-api-types";
import { useMoments, type MomentView } from "../features/moments/useMoments";
import { useAuthStore } from "../stores/authStore";

const visibilityOptions = [{ value: 2 as const, label: "好友可见", icon: Users }, { value: 3 as const, label: "仅自己可见", icon: UsersRound }];

export function MomentsPage() {
  const [composerOpen, setComposerOpen] = useState(false);
  const [content, setContent] = useState("");
  const [visibility, setVisibility] = useState<2 | 3>(2);
  const [feedMode, setFeedMode] = useState<"friends" | "mine">("friends");
  const [visibilityOpen, setVisibilityOpen] = useState(false);
  const [commentingId, setCommentingId] = useState<number | null>(null);
  const [commentDrafts, setCommentDrafts] = useState<Record<number, string>>({});
  const [deletePostId, setDeletePostId] = useState<number | null>(null);
  const [likersPost, setLikersPost] = useState<MomentView | null>(null);
  const [likers, setLikers] = useState<MomentLiker[]>([]);
  const [likersLoading, setLikersLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const reduceMotion = useReducedMotion();
  const user = useAuthStore((state) => state.user);
  const username = user?.username ?? "用户";
  const { posts, publish, toggleLike, addComment, deleteComment, getLikers, deleteMoment, isLoading, isError, hasNextPage, fetchNextPage, isFetchingNextPage, isMutating } = useMoments(feedMode);

  const run = async (operation: () => Promise<void>) => {
    setError(null);
    try { await operation(); }
    catch (failure) { setError(failure instanceof ApiError ? failure.message : "操作失败，请稍后重试"); }
  };
  const submitPost = () => void run(async () => { await publish(content.trim(), visibility); setContent(""); setComposerOpen(false); });
  const submitComment = (post: MomentView) => { const value = commentDrafts[post.id]?.trim(); if (!value) return; void run(async () => { await addComment(post.id, value); setCommentDrafts((current) => ({ ...current, [post.id]: "" })); setCommentingId(null); }); };
  const openLikers = (post: MomentView) => void run(async () => { setLikersPost(post); setLikers([]); setLikersLoading(true); try { setLikers(await getLikers(post)); } finally { setLikersLoading(false); } });
  const confirmDelete = () => { if (deletePostId === null) return; void run(async () => { await deleteMoment(deletePostId); setDeletePostId(null); }); };
  const selectedVisibility = visibilityOptions.find((item) => item.value === visibility)!;
  const VisibilityIcon = selectedVisibility.icon;

  return <><aside className="module-sidebar moments-sidebar"><header className="module-sidebar__header"><div><span className="eyebrow">Moments</span><h1>朋友圈</h1></div></header><div className="moment-me"><Avatar name={username} online size="lg" src={user?.avatarUrl} /><div><strong>{username}</strong><small>记录生活，也保持连接。</small></div></div><Button leadingIcon={<PenLine size={16} />} onClick={() => setComposerOpen(true)}>发布动态</Button><div className="moment-filter-list"><button className={feedMode === "friends" ? "is-active" : ""} onClick={() => setFeedMode("friends")}><UsersRound size={17} /><span>好友动态</span>{feedMode === "friends" && <small>{posts.length}</small>}</button><button className={feedMode === "mine" ? "is-active" : ""} onClick={() => setFeedMode("mine")}><Users size={17} /><span>我的动态</span>{feedMode === "mine" && <small>{posts.length}</small>}</button></div></aside>
    <section className="module-main moments-main"><header className="feed-header"><div><span className="eyebrow">{feedMode === "mine" ? "My moments" : "Friends feed"}</span><h2>{feedMode === "mine" ? "我的动态" : "最近动态"}</h2></div><p>{feedMode === "mine" ? "查看自己发布过的内容。" : "今天也有一些值得分享的事。"}</p></header>
      <AnimatePresence>{composerOpen && <motion.div animate={{ opacity: 1, height: "auto", y: 0 }} className="moment-composer" exit={{ opacity: 0, height: 0 }} initial={{ opacity: 0, height: 0, y: reduceMotion ? 0 : -8 }}><div className="moment-composer__top"><Avatar name={username} src={user?.avatarUrl} /><textarea autoFocus maxLength={2000} onChange={(event) => setContent(event.target.value)} placeholder="分享此刻的想法…" rows={3} value={content} /></div><footer><div className="visibility-picker"><button onClick={() => setVisibilityOpen((value) => !value)}><VisibilityIcon size={15} />{selectedVisibility.label}<ChevronDown size={14} /></button>{visibilityOpen && <div>{visibilityOptions.map((item) => { const Icon = item.icon; return <button key={item.value} onClick={() => { setVisibility(item.value); setVisibilityOpen(false); }}><Icon size={14} />{item.label}</button>; })}</div>}</div><span><Button onClick={() => { setContent(""); setComposerOpen(false); }} size="sm" variant="ghost">取消</Button><button aria-label="发布动态" className="composer__send" disabled={!content.trim() || isMutating} onClick={submitPost} type="button"><ArrowUp size={20} /></button></span></footer></motion.div>}</AnimatePresence>
      {error && <div className="feed-error"><CircleAlert size={15} />{error}</div>}
      {isLoading ? <div className="feed-loading"><LoaderCircle className="ui-spinner" size={20} />正在加载动态</div> : isError ? <div className="feed-loading"><CircleAlert size={20} />动态加载失败</div> : <div className="moment-feed">{posts.map((post, index) => { const mine = post.author_id === user?.id; return <motion.article animate={{ opacity: 1, y: 0 }} className="moment-card" initial={{ opacity: 0, y: reduceMotion ? 0 : 10 }} key={post.id} transition={{ delay: reduceMotion ? 0 : index * .04 }}><header><Avatar name={post.author} src={post.avatar} /><div><strong>{post.author}</strong><time>{post.time}</time></div></header><p className="moment-card__content">{post.content}</p><div className="moment-card__actions"><button className={post.liked_by_me ? "is-liked" : ""} disabled={isMutating} onClick={() => void run(() => toggleLike(post))}><Heart fill={post.liked_by_me ? "currentColor" : "none"} size={17} />{post.like_count}</button><button onClick={() => setCommentingId(commentingId === post.id ? null : post.id)}><MessageCircle size={17} />{post.comments.length}</button><button className="moment-likers-trigger" onClick={() => openLikers(post)} type="button">查看点赞</button>{mine && <button className="moment-delete-trigger" disabled={isMutating} onClick={() => setDeletePostId(post.id)} type="button"><Trash2 size={16} />删除动态</button>}</div>{post.comments.length > 0 && <div className="moment-interactions">{post.comments.map((comment) => <p className="moment-comment" key={comment.id}><strong>{comment.author}</strong><span>{comment.content}</span>{comment.mine && <button aria-label="删除评论" onClick={() => void run(() => deleteComment(post.id, comment.id))}><Trash2 size={11} /></button>}</p>)}</div>}{commentingId === post.id && <div className="comment-composer"><input autoFocus maxLength={2000} onChange={(event) => setCommentDrafts((current) => ({ ...current, [post.id]: event.target.value }))} onKeyDown={(event) => { if (event.key === "Enter") submitComment(post); }} placeholder="写下评论，按 Enter 发送" value={commentDrafts[post.id] ?? ""} /><Button disabled={!commentDrafts[post.id]?.trim() || isMutating} onClick={() => submitComment(post)} size="sm">发送</Button></div>}</motion.article>; })}{posts.length === 0 && <div className="feed-loading">还没有动态，发布第一条吧。</div>}{hasNextPage && <Button loading={isFetchingNextPage} onClick={() => void fetchNextPage()} variant="secondary">加载更多</Button>}</div>}
      <Drawer description={likersPost ? `共 ${likersPost.like_count} 人点赞` : undefined} onClose={() => setLikersPost(null)} open={Boolean(likersPost)} title="点赞的人">{likersLoading ? <div className="likers-loading"><LoaderCircle className="ui-spinner" size={18} />正在加载点赞列表</div> : likers.length === 0 ? <div className="likers-loading">暂时没有点赞</div> : <div className="likers-list">{likers.map((liker) => <div key={liker.user_id}><Avatar name={liker.username} size="sm" src={liker.avatar_url} /><strong>{liker.username}</strong></div>)}</div>}</Drawer>
      <ConfirmDialog confirmLabel="删除动态" description="删除后，动态及其点赞和评论将无法恢复。" destructive onClose={() => setDeletePostId(null)} onConfirm={confirmDelete} open={deletePostId !== null} title="删除这条动态？" />
    </section></>;
}
