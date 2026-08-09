import { create } from "zustand";
import type { ConvSummary, InboxMessage, SendMessage, ServerAck, SyncBatch } from "../../goim-ws-types";
import { ConvType, MsgType } from "../../goim-ws-types";
import { conversations as previewConversations, messagesByConversation as previewMessages } from "../mocks/data";
import { goimSocket, type ConnectionState } from "../realtime/socket";

export interface ChatConversation {
  id: string;
  name: string;
  preview: string;
  time: string;
  unread: number;
  targetId: number;
  convType: ConvType;
  online?: boolean;
  muted?: boolean;
  group?: boolean;
  avatarUrl?: string;
}

export interface ChatMessage {
  id: string;
  clientMsgId?: string;
  serverMsgId?: number;
  /** 群消息序号（群聊专属）：用作已读水位线（我最后看到第几条） */
  groupSeq?: number;
  convId: string;
  from: "me" | "other";
  senderId: number;
  content: string;
  time: string;
  timestamp: number;
  status: "sent" | "pending" | "failed" | "revoked";
  outbound?: SendMessage;
  error?: string;
}

interface ChatState {
  mode: "preview" | "live" | null;
  liveUserId: number | null;
  connectionState: ConnectionState;
  syncCompleted: boolean;
  conversations: ChatConversation[];
  messagesByConversation: Record<string, ChatMessage[]>;
  lastSyncTime: number;
  lastSyncMsgId: number;
  initializePreview: () => void;
  initializeLive: (userId: number) => void;
  resetSession: () => void;
  setConnectionState: (state: ConnectionState) => void;
  sendText: (conversation: ChatConversation, content: string, preview: boolean) => void;
  retryMessage: (convId: string, messageId: string, preview: boolean) => void;
  markConversationRead: (convId: string) => void;
  acknowledge: (ack: ServerAck) => void;
  failLatestPending: (message: string) => void;
  receiveMessage: (message: InboxMessage, currentUserId: number) => void;
  setConversationIdentity: (convId: string, name: string, avatarUrl?: string, online?: boolean) => void;
  applySyncBatch: (batch: SyncBatch, currentUserId: number) => void;
  applyConversationSync: (summaries: ConvSummary[], unreadMap: Record<string, number>) => void;
  revokeMessage: (convId: string, serverMsgId: number) => void;
  addGroupConversation: (groupId: number, name: string) => void;
  addPrivateConversation: (id: string, targetId: number, name: string, avatarUrl?: string) => void;
  setConversationMuted: (convId: string, muted: boolean) => void;
  removeConversation: (convId: string) => void;
}

const previewTargets: Record<string, number> = {
  "lin-cheng": 2001,
  "product-team": 3001,
  "zhou-yu": 2002,
  "design-room": 3002,
  "chen-xi": 2003,
  "lu-yao": 2004,
};

function formatTime(timestamp: number) {
  return new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false }).format(timestamp);
}

function previewState() {
  const conversations: ChatConversation[] = previewConversations.map((item) => ({
    ...item,
    targetId: previewTargets[item.id],
    convType: item.group ? ConvType.Group : ConvType.Private,
  }));
  const messagesByConversation: Record<string, ChatMessage[]> = {};
  for (const [convId, messages] of Object.entries(previewMessages)) {
    messagesByConversation[convId] = messages.map((message, index) => ({
      id: `preview-${convId}-${message.id}`,
      convId,
      from: message.from,
      senderId: message.from === "me" ? 0 : -1,
      content: message.content,
      time: message.time,
      timestamp: Date.now() - (messages.length - index) * 60_000,
      status: message.status ?? "sent",
    }));
  }
  return { conversations, messagesByConversation };
}

function serverMessageToChat(message: InboxMessage, currentUserId: number): ChatMessage {
  return {
    id: `server-${message.msgId}`,
    serverMsgId: message.msgId,
    groupSeq: message.groupSeq,
    convId: message.convId,
    from: message.fromId === currentUserId ? "me" : "other",
    senderId: message.fromId,
    content: message.content,
    time: formatTime(message.timestamp),
    timestamp: message.timestamp,
    status: message.msgType === MsgType.Revoked ? "revoked" : "sent",
  };
}

function mergeServerMessages(existing: ChatMessage[], incoming: ChatMessage[]) {
  const seen = new Set(existing.map((message) => message.serverMsgId).filter(Boolean));
  const merged = [...existing];
  for (const message of incoming) {
    if (message.serverMsgId && seen.has(message.serverMsgId)) continue;
    if (message.serverMsgId) seen.add(message.serverMsgId);
    merged.push(message);
  }
  return merged.sort((a, b) => a.timestamp - b.timestamp);
}

export const useChatStore = create<ChatState>((set, get) => ({
  mode: null,
  liveUserId: null,
  connectionState: "idle",
  syncCompleted: false,
  conversations: [],
  messagesByConversation: {},
  lastSyncTime: 0,
  lastSyncMsgId: 0,

  initializePreview: () => {
    if (get().mode === "preview") return;
    set({ mode: "preview", liveUserId: null, connectionState: "connected", syncCompleted: true, lastSyncTime: Date.now(), lastSyncMsgId: 0, ...previewState() });
  },
  initializeLive: (userId) => {
    if (get().mode === "live" && get().liveUserId === userId) return;
    set({ mode: "live", liveUserId: userId, connectionState: "connecting", syncCompleted: false, conversations: [], messagesByConversation: {}, lastSyncTime: 0, lastSyncMsgId: 0 });
  },
  resetSession: () => set({ mode: null, liveUserId: null, connectionState: "idle", syncCompleted: false, conversations: [], messagesByConversation: {}, lastSyncTime: 0, lastSyncMsgId: 0 }),
  setConnectionState: (connectionState) => set({ connectionState }),

  sendText: (conversation, content, preview) => {
    const clientMsgId = crypto.randomUUID();
    const timestamp = Date.now();
    const outbound: SendMessage = { msgId: clientMsgId, convType: conversation.convType, toId: conversation.targetId, msgType: MsgType.Text, content, timestamp };
    const localMessage: ChatMessage = { id: `client-${clientMsgId}`, clientMsgId, convId: conversation.id, from: "me", senderId: get().liveUserId ?? 0, content, time: formatTime(timestamp), timestamp, status: "pending", outbound };
    set((state) => ({
      messagesByConversation: { ...state.messagesByConversation, [conversation.id]: [...(state.messagesByConversation[conversation.id] ?? []), localMessage] },
      conversations: [
        ...state.conversations
          .filter((item) => item.id === conversation.id)
          .map((item) => ({ ...item, preview: content, time: "刚刚" })),
        ...state.conversations.filter((item) => item.id !== conversation.id),
      ],
    }));

    if (preview) {
      window.setTimeout(() => get().acknowledge({ clientMsgId, serverMsgId: timestamp, timestamp: Date.now() }), 420);
      return;
    }
    if (!goimSocket.send({ type: "msg", data: outbound })) {
      set((state) => ({ messagesByConversation: { ...state.messagesByConversation, [conversation.id]: state.messagesByConversation[conversation.id].map((message) => message.id === localMessage.id ? { ...message, status: "failed", error: "连接不可用" } : message) } }));
    }
  },

  retryMessage: (convId, messageId, preview) => {
    const message = get().messagesByConversation[convId]?.find((item) => item.id === messageId);
    if (!message?.outbound) return;
    set((state) => ({ messagesByConversation: { ...state.messagesByConversation, [convId]: state.messagesByConversation[convId].map((item) => item.id === messageId ? { ...item, status: "pending", error: undefined } : item) } }));
    if (preview) {
      window.setTimeout(() => get().acknowledge({ clientMsgId: message.outbound!.msgId, serverMsgId: Date.now(), timestamp: Date.now() }), 420);
    } else if (!goimSocket.send({ type: "msg", data: message.outbound })) {
      set((state) => ({ messagesByConversation: { ...state.messagesByConversation, [convId]: state.messagesByConversation[convId].map((item) => item.id === messageId ? { ...item, status: "failed", error: "连接不可用" } : item) } }));
    }
  },

  markConversationRead: (convId) => set((state) => {
    const conversation = state.conversations.find((item) => item.id === convId);
    if (!conversation || conversation.unread === 0) return state;
    return {
      conversations: state.conversations.map((item) => item.id === convId ? { ...item, unread: 0 } : item),
    };
  }),

  acknowledge: (ack) => set((state) => ({
    messagesByConversation: Object.fromEntries(Object.entries(state.messagesByConversation).map(([convId, messages]) => [convId, messages.map((message) => message.clientMsgId === ack.clientMsgId ? { ...message, serverMsgId: ack.serverMsgId, groupSeq: ack.groupSeq ?? message.groupSeq, status: "sent" as const, error: undefined } : message)])),
  })),

  failLatestPending: (error) => set((state) => {
    let newest: ChatMessage | undefined;
    for (const messages of Object.values(state.messagesByConversation)) for (const message of messages) if (message.status === "pending" && (!newest || message.timestamp > newest.timestamp)) newest = message;
    if (!newest) return state;
    return { messagesByConversation: { ...state.messagesByConversation, [newest.convId]: state.messagesByConversation[newest.convId].map((message) => message.id === newest!.id ? { ...message, status: "failed", error } : message) } };
  }),

  receiveMessage: (message, currentUserId) => set((state) => {
    const converted = serverMessageToChat(message, currentUserId);
    const current = state.messagesByConversation[message.convId] ?? [];
    const exists = current.some((item) => item.serverMsgId === message.msgId);
    if (exists) return state;
    const existingConversation = state.conversations.find((conversation) => conversation.id === message.convId);
    const targetId = message.convType === ConvType.Group
      ? message.toId
      : message.fromId === currentUserId ? message.toId : message.fromId;
    const updatedConversation: ChatConversation = existingConversation
      ? { ...existingConversation, preview: message.content, time: formatTime(message.timestamp), unread: existingConversation.unread + (message.fromId === currentUserId ? 0 : 1) }
      : { id: message.convId, name: message.convType === ConvType.Group ? `群聊 #${message.toId}` : `用户 #${targetId}`, preview: message.content, time: formatTime(message.timestamp), unread: message.fromId === currentUserId ? 0 : 1, targetId, convType: message.convType, group: message.convType === ConvType.Group };
    return {
      messagesByConversation: { ...state.messagesByConversation, [message.convId]: mergeServerMessages(current, [converted]) },
      conversations: [updatedConversation, ...state.conversations.filter((conversation) => conversation.id !== message.convId)],
    };
  }),
  setConversationIdentity: (convId, name, avatarUrl, online) => set((state) => ({ conversations: state.conversations.map((conversation) => conversation.id === convId ? { ...conversation, name, avatarUrl, online: online ?? conversation.online } : conversation) })),

  applySyncBatch: (batch, currentUserId) => set((state) => {
    const next = { ...state.messagesByConversation };
    for (const message of batch.msgs) {
      const converted = serverMessageToChat(message, currentUserId);
      next[message.convId] = mergeServerMessages(next[message.convId] ?? [], [converted]);
    }
    return {
      messagesByConversation: next,
      lastSyncTime: batch.syncTime || state.lastSyncTime,
      lastSyncMsgId: batch.syncMsgId ?? 0,
    };
  }),

  applyConversationSync: (summaries, unreadMap) => set((state) => ({
    syncCompleted: true,
    conversations: [...summaries.map((summary) => ({
      id: summary.convId,
      name: summary.targetName || `会话 ${summary.targetId}`,
      preview: summary.lastMsg,
      time: summary.lastMsgTime ? formatTime(summary.lastMsgTime) : "",
      unread: unreadMap[summary.convId] ?? 0,
      targetId: summary.targetId,
      convType: summary.convType,
      group: summary.convType === ConvType.Group,
      avatarUrl: summary.targetAvatar || undefined,
      muted: state.conversations.find((item) => item.id === summary.convId)?.muted,
    }))],
  })),

  revokeMessage: (convId, serverMsgId) => set((state) => ({
    messagesByConversation: { ...state.messagesByConversation, [convId]: (state.messagesByConversation[convId] ?? []).map((message) => message.serverMsgId === serverMsgId ? { ...message, content: "消息已撤回", status: "revoked" } : message) },
  })),

  addGroupConversation: (groupId, name) => set((state) => {
    const id = state.mode === "preview" ? `preview-group-${groupId}` : `g_${groupId}`;
    if (state.conversations.some((conversation) => conversation.id === id)) {
      return { conversations: state.conversations.map((conversation) => conversation.id === id ? { ...conversation, name, targetId: groupId, convType: ConvType.Group, group: true } : conversation) };
    }
    return {
      conversations: [{ id, name, preview: "群聊已创建", time: "刚刚", unread: 0, targetId: groupId, convType: ConvType.Group, group: true }, ...state.conversations],
      messagesByConversation: { ...state.messagesByConversation, [id]: [] },
    };
  }),
  addPrivateConversation: (id, targetId, name, avatarUrl) => set((state) => {
    if (state.conversations.some((conversation) => conversation.id === id)) return state;
    return {
      conversations: [{ id, name, avatarUrl, preview: "还没有消息，打个招呼吧", time: "", unread: 0, targetId, convType: ConvType.Private }, ...state.conversations],
      messagesByConversation: { ...state.messagesByConversation, [id]: [] },
    };
  }),
  setConversationMuted: (convId, muted) => set((state) => {
    const conversation = state.conversations.find((item) => item.id === convId);
    if (!conversation || Boolean(conversation.muted) === muted) return state;
    return { conversations: state.conversations.map((item) => item.id === convId ? { ...item, muted } : item) };
  }),
  removeConversation: (convId) => set((state) => {
    const messagesByConversation = { ...state.messagesByConversation };
    delete messagesByConversation[convId];
    return { conversations: state.conversations.filter((conversation) => conversation.id !== convId), messagesByConversation };
  }),
}));
