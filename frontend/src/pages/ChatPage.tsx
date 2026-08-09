import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { useQuery } from "@tanstack/react-query";
import { ArrowLeft, ArrowUp, BellOff, CheckCheck, CircleAlert, LoaderCircle, MessageCircle, MoreHorizontal, Plus, RotateCcw, Search, WifiOff } from "lucide-react";
import { useEffect, useLayoutEffect, useRef, useState, type KeyboardEvent } from "react";
import { NavLink, useNavigate, useParams } from "react-router-dom";
import { Avatar, Badge, Drawer, EmptyState, IconButton, Switch, TextField } from "../components/ui";
import { goimSocket } from "../realtime/socket";
import { useAuthStore } from "../stores/authStore";
import { useChatStore, type ChatMessage } from "../stores/chatStore";
import { CreateGroupDrawer, GroupManagementDrawer } from "../features/groups/GroupManagement";
import { groupsApi, settingsApi } from "../lib/api";

const emptyMessages: ChatMessage[] = [];

const connectionLabels = {
  idle: "未连接",
  connecting: "正在连接",
  connected: "已连接",
  reconnecting: "正在重新连接",
  offline: "连接中断",
} as const;

export function ChatPage() {
  const { conversationId } = useParams();
  const navigate = useNavigate();
  const conversations = useChatStore((state) => state.conversations);
  const messages = useChatStore((state) => state.messagesByConversation[conversationId ?? ""] ?? emptyMessages);
  const connectionState = useChatStore((state) => state.connectionState);
  const syncCompleted = useChatStore((state) => state.syncCompleted);
  const mode = useChatStore((state) => state.mode);
  const sendText = useChatStore((state) => state.sendText);
  const retryMessage = useChatStore((state) => state.retryMessage);
  const markConversationRead = useChatStore((state) => state.markConversationRead);
  const setConversationMuted = useChatStore((state) => state.setConversationMuted);
  const previewMode = useAuthStore((state) => state.previewMode);
  const selected = conversations.find((item) => item.id === conversationId);
  const selectedId = selected?.id;
  const groupMembersQuery = useQuery({
    queryKey: ["group-members", selected?.targetId],
    queryFn: () => groupsApi.members(selected!.targetId, 500, 0),
    enabled: Boolean(selected?.group && !previewMode),
  });
  const groupMembers = groupMembersQuery.data?.items ?? [];
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [createGroupOpen, setCreateGroupOpen] = useState(false);
  const [draft, setDraft] = useState("");
  const [muteSaving, setMuteSaving] = useState(false);
  const [muteError, setMuteError] = useState<string | null>(null);
  const reduceMotion = useReducedMotion();
  const messageScrollRef = useRef<HTMLDivElement>(null);
  const initiallyPositionedConversationRef = useRef<string | null>(null);

  useEffect(() => {
    if (conversationId && mode === "live" && conversations.length > 0 && !selected) navigate(`/app/chats/${conversations[0].id}`, { replace: true });
  }, [conversationId, conversations, mode, navigate, selected]);

  useEffect(() => {
    if (!selectedId) return;
    markConversationRead(selectedId);
    if (!previewMode && connectionState === "connected") {
      // 群聊已读水位线：携带最后一条已展示群消息的序号，服务端据此精确定位"读到第几条"。
      // 私聊无 groupSeq，不携带该字段（服务端按原行为清零计数）。
      const lastGroupSeq = messages.reduce((max, message) => (message.groupSeq ? Math.max(max, message.groupSeq) : max), 0);
      goimSocket.send({ type: "readAck", data: lastGroupSeq > 0 ? { convId: selectedId, lastReadGroupSeq: lastGroupSeq } : { convId: selectedId } });
    }
  }, [connectionState, markConversationRead, messages, previewMode, selectedId]);

  useLayoutEffect(() => {
    if (!selectedId || (mode === "live" && !syncCompleted)) return;
    const scrollContainer = messageScrollRef.current;
    if (!scrollContainer) return;

    const isInitialPosition = initiallyPositionedConversationRef.current !== selectedId;
    if (isInitialPosition) {
      scrollContainer.scrollTop = scrollContainer.scrollHeight;
      initiallyPositionedConversationRef.current = selectedId;
      return;
    }
    scrollContainer.scrollTo({ top: scrollContainer.scrollHeight, behavior: reduceMotion ? "auto" : "smooth" });
  }, [messages.length, mode, reduceMotion, selectedId, syncCompleted]);

  const sendMessage = () => {
    const content = draft.trim();
    if (!content || !selected) return;
    sendText(selected, content, previewMode);
    setDraft("");
  };

  const handleComposerKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      sendMessage();
    }
  };

  const revokeMessage = (message: ChatMessage) => {
    if (!message.serverMsgId || !selected) return;
    goimSocket.send({ type: "revokeMsg", data: { convId: selected.id, serverMsgId: message.serverMsgId } });
  };

  const toggleMute = async (muted: boolean) => {
    if (!selected || muteSaving) return;
    setMuteError(null);
    setMuteSaving(true);
    try {
      if (!previewMode) {
        if (muted) await settingsApi.mute({ convId: selected.id });
        else await settingsApi.unmute(selected.id);
      }
      setConversationMuted(selected.id, muted);
    } catch {
      setMuteError("设置免打扰失败，请稍后重试。");
    } finally {
      setMuteSaving(false);
    }
  };

  return (
    <>
      <aside className={`module-sidebar conversation-sidebar ${conversationId ? "conversation-sidebar--mobile-hidden" : ""}`}>
        <header className="module-sidebar__header"><div><span className="eyebrow">Messages</span><h1>聊天</h1></div><IconButton label="创建群聊" onClick={() => setCreateGroupOpen(true)}><Plus size={18} /></IconButton></header>
        <div className={`connection-status connection-status--${connectionState}`}><span />{previewMode ? "预览连接" : connectionLabels[connectionState]}</div>
        <div className="module-sidebar__search"><TextField aria-label="搜索会话" leadingIcon={<Search size={16} />} placeholder="搜索会话" /></div>
        <div className="sidebar-section-label"><span>最近会话</span><small>{conversations.length}</small></div>
        <nav aria-label="会话列表" className="conversation-nav">
          {conversations.map((item) => (
            <NavLink className={({ isActive }) => isActive ? "conversation-item is-active" : "conversation-item"} key={item.id} to={`/app/chats/${item.id}`}>
              <Avatar name={item.name} online={item.online} src={item.avatarUrl} />
              <span className="conversation-item__copy"><strong>{item.name}</strong><small>{item.preview}</small></span>
              <span className="conversation-item__meta"><time>{item.time}</time>{item.unread > 0 ? <b>{item.unread}</b> : item.muted ? <BellOff size={12} /> : null}</span>
            </NavLink>
          ))}
        </nav>
      </aside>

      <section className={`module-main chat-main ${conversationId ? "" : "chat-main--mobile-hidden"}`}>
        {selected ? (
          <>
            <header className="chat-header">
              <IconButton label="返回会话列表" className="chat-header__back" onClick={() => navigate("/app/chats")}><ArrowLeft size={19} /></IconButton>
              <div className="chat-header__person"><Avatar name={selected.name} online={selected.online} src={selected.avatarUrl} /><div><h2>{selected.name}</h2><p>{selected.group ? "群聊" : selected.online ? "在线" : "离线"}</p></div></div>
              <div className="chat-header__actions">
                <IconButton label="查看聊天资料" onClick={() => setDrawerOpen(true)}><MoreHorizontal size={19} /></IconButton>
              </div>
            </header>
            {connectionState !== "connected" && !previewMode && <div className="chat-connection-banner"><WifiOff size={14} /><span>{connectionLabels[connectionState]}，未确认的消息会保留并允许重试。</span></div>}
            <div className="message-scroll" ref={messageScrollRef}>
              <div className="message-day"><span>今天</span></div>
              {messages.length === 0 && <div className="conversation-empty"><MessageCircle size={21} /><p>还没有消息，发一句问候吧。</p></div>}
              <AnimatePresence initial={false}>
                {messages.map((message) => (
                  <motion.div animate={{ opacity: 1, y: 0 }} className={`message-row ${message.from === "me" ? "is-me" : ""} ${message.status === "revoked" ? "is-revoked" : ""}`} initial={{ opacity: 0, y: reduceMotion ? 0 : 8 }} key={message.id} transition={{ duration: reduceMotion ? 0 : .2 }}>
                    {message.from === "other" && (() => {
                      const sender = selected.group ? groupMembers.find((member) => member.user_id === message.senderId) : undefined;
                      return <Avatar name={sender?.username ?? (selected.group ? `用户 ${message.senderId}` : selected.name)} size="sm" src={selected.group ? sender?.avatar_url : selected.avatarUrl} />;
                    })()}
                    <div className="message-stack">
                      <div className="message-bubble">{message.content}</div>
                      <span className={`message-time message-time--${message.status}`}>
                        {message.time}
                        {message.from === "me" && message.status === "sent" && <CheckCheck size={12} />}
                        {message.status === "pending" && <LoaderCircle className="ui-spinner" size={11} />}
                        {message.status === "failed" && <button aria-label="重新发送" onClick={() => retryMessage(selected.id, message.id, previewMode)} title={message.error}><CircleAlert size={11} />发送失败<RotateCcw size={10} /></button>}
                        {message.from === "me" && message.status === "sent" && message.serverMsgId && Date.now() - message.timestamp <= 120_000 && <button className="message-revoke" onClick={() => revokeMessage(message)} type="button">撤回</button>}
                      </span>
                    </div>
                  </motion.div>
                ))}
              </AnimatePresence>
            </div>
            <footer className="composer-wrap"><div className="composer"><textarea aria-label="输入消息" maxLength={2000} onChange={(event) => setDraft(event.target.value)} onKeyDown={handleComposerKeyDown} placeholder="输入消息…" rows={1} value={draft} /><span className="composer__count">{draft.length}/2000</span><button aria-label="发送消息" className="composer__send" disabled={!draft.trim()} onClick={sendMessage} type="button"><ArrowUp size={20} /></button></div></footer>
          </>
        ) : mode === "live" && !syncCompleted ? (
          <EmptyState description="连接成功后，会话和离线消息将在这里同步。" icon={<LoaderCircle className="ui-spinner" size={25} />} title="正在同步会话" />
        ) : mode === "live" && conversations.length === 0 ? (
          <EmptyState description="添加好友或创建群聊后，就可以开始第一段对话。" icon={<MessageCircle size={25} />} title="暂无会话" />
        ) : (
          <EmptyState description="从左侧选择一个会话，开始查看消息。" icon={<MessageCircle size={25} />} title="选择一段对话" />
        )}
      </section>

      {selected?.group ? <GroupManagementDrawer conversation={selected} onClose={() => setDrawerOpen(false)} open={drawerOpen} /> : <Drawer description="联系人资料与会话设置" onClose={() => setDrawerOpen(false)} open={drawerOpen} title={selected?.name ?? "聊天资料"}>
        <div className="profile-hero"><Avatar name={selected?.name ?? "?"} online={selected?.online} size="xl" src={selected?.avatarUrl} /><h3>{selected?.name}</h3><p>{selected?.group ? "群聊资料将在群组阶段接入" : "联系人基础资料"}</p>{selected?.group && <Badge>群聊</Badge>}</div>
        <div className="group-notification-setting"><Switch checked={Boolean(selected?.muted)} description="开启后，该会话不会触发声音与桌面通知。" disabled={muteSaving} label="消息免打扰" onCheckedChange={(checked) => void toggleMute(checked)} />{muteError && <p className="drawer-setting-error">{muteError}</p>}</div>
      </Drawer>}
      <CreateGroupDrawer onClose={() => setCreateGroupOpen(false)} onCreated={(id) => navigate(`/app/chats/${id}`)} open={createGroupOpen} />
    </>
  );
}
