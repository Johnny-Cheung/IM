import {
  Fragment,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
  type KeyboardEvent,
} from "react";
import { Link, useNavigate, useParams, type NavigateOptions, type To } from "react-router-dom";
import {
  api,
  ApiError,
  describeError,
  isSessionError,
  websocketURL,
  type ChatMessage,
  type Conversation,
  type DirectConversation,
  type GroupConversation,
  type GroupDetail,
  type GroupMessage,
  type GroupReadStats,
  type PublicUser,
} from "../api";
import {
  readConversations,
  readFriends,
  readMessages,
  readSyncState,
  removeMessages,
  writeConversations,
  writeFriends,
  writeMessages,
} from "../cache";
import type { CachedMessage } from "../cache";
import { BLUR_FRAME, focusFrame, parseFocusAck, type FocusAck, type FocusTarget } from "../focus";
import { groupReceiptLabel, lastOwnSentMessageID as findLastOwnSentMessageID } from "../groupReceipt";
import {
  Debouncer,
  READ_STATS_DEBOUNCE_MS,
  ReadStatsGate,
  parseGroupReadEvent,
  parseGroupReadStats,
  type GroupReadEvent,
} from "../readStats";
import { Brand } from "../components/Brand";
import { useAuth } from "../auth";

const PAGE_SIZE = 30;
const MAX_MESSAGE_LENGTH = 2000;
const MAX_GROUP_NAME_LENGTH = 30;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_BASE_DELAY = 1000;
const RECONNECT_MAX_DELAY = 10000;
const WS_CLOSE_REPLACED = 4001;
const WS_CLOSE_SESSION = 4002;

type ConnectionStatus = "connecting" | "open" | "closed" | "replaced";
type MessageStatus = "sent" | "sending" | "failed";
type DirectDisplayMessage = ChatMessage & { status: MessageStatus };
type GroupDisplayMessage = GroupMessage & { status: MessageStatus };
type DisplayMessage = DirectDisplayMessage | GroupDisplayMessage;

type ActiveTarget =
  | { kind: "direct"; key: string; username: string }
  | { kind: "group"; key: string; groupID: number };

type ReadEvent = {
  reader: string;
  peer: string;
  lastReadMessageID: number;
};

type GroupChangedEvent = {
  groupID: number;
  action: string;
};

type GroupReceipt = {
  groupID: number;
  messageID: number;
  stats: GroupReadStats;
};

type ScrollPosition = {
  height: number;
  top: number;
};

function sameUsername(left: string, right: string): boolean {
  return left.toLocaleLowerCase() === right.toLocaleLowerCase();
}

const messageTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

const conversationTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

const dayFormatter = new Intl.DateTimeFormat("zh-CN", {
  year: "numeric",
  month: "long",
  day: "numeric",
});

function directKey(username: string): string {
  return `direct:${username.toLocaleLowerCase()}`;
}

function groupKey(groupID: number): string {
  return `group:${groupID}`;
}

function conversationKey(conversation: Conversation): string {
  return conversation.type === "direct"
    ? directKey(conversation.user.username)
    : groupKey(conversation.group.id);
}

function isGroupMessage(message: DisplayMessage): message is GroupDisplayMessage {
  return "group_id" in message;
}

function initialOf(name: string): string {
  return [...name][0] ?? "?";
}

function newClientMessageID(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  const bytes = new Uint8Array(16);
  if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
    crypto.getRandomValues(bytes);
  } else {
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
}

function toDirectDisplay(message: ChatMessage): DirectDisplayMessage {
  return { ...message, status: "sent" };
}

function toGroupDisplay(message: GroupMessage): GroupDisplayMessage {
  return { ...message, status: "sent" };
}

function toDisplayMessage(message: CachedMessage): DisplayMessage {
  return "group_id" in message
    ? toGroupDisplay(message)
    : toDirectDisplay(message);
}

function sortMessages(messages: DisplayMessage[]): DisplayMessage[] {
  return [...messages].sort((left, right) => {
    if (left.status === "sent" && right.status === "sent") {
      return left.id - right.id;
    }
    if (left.status === "sent") {
      return -1;
    }
    if (right.status === "sent") {
      return 1;
    }
    return 0;
  });
}

function mergeMessages(
  existing: DisplayMessage[],
  incoming: DisplayMessage[],
): DisplayMessage[] {
  const merged = [...existing];
  for (const message of incoming) {
    const index = merged.findIndex((item) =>
      item.client_message_id === message.client_message_id
      || (message.id > 0 && item.id === message.id));
    if (index >= 0) {
      merged[index] = message;
    } else {
      merged.push(message);
    }
  }
  return sortMessages(merged);
}

function conversationTitle(conversation: Conversation): string {
  return conversation.type === "direct"
    ? conversation.user.nickname
    : conversation.group.name;
}

function sortConversations(conversations: Conversation[]): Conversation[] {
  return [...conversations].sort((left, right) => {
    const leftTime = left.last_message
      ? Date.parse(left.last_message.created_at) || 0
      : 0;
    const rightTime = right.last_message
      ? Date.parse(right.last_message.created_at) || 0
      : 0;
    if (leftTime !== rightTime) {
      return rightTime - leftTime;
    }
    if (left.last_message && right.last_message) {
      return conversationTitle(left).localeCompare(conversationTitle(right));
    }
    if (left.last_message) {
      return -1;
    }
    if (right.last_message) {
      return 1;
    }
    if (left.type !== right.type) {
      return left.type === "direct" ? -1 : 1;
    }
    if (left.type === "direct" && right.type === "direct") {
      const leftName = left.user.nickname.toLocaleLowerCase();
      const rightName = right.user.nickname.toLocaleLowerCase();
      if (leftName !== rightName) {
        return leftName < rightName ? -1 : 1;
      }
      return left.user.username.localeCompare(right.user.username);
    }
    if (left.type === "group" && right.type === "group") {
      return (Date.parse(right.group.created_at) || 0)
        - (Date.parse(left.group.created_at) || 0);
    }
    return 0;
  });
}

function previewOf(conversation: Conversation, currentUsername: string): string {
  const message = conversation.last_message;
  if (!message) {
    return conversation.type === "group" ? "群聊已创建" : "还没有消息";
  }
  const prefix = sameUsername(message.sender.username, currentUsername)
    ? "我："
    : conversation.type === "group"
      ? `${message.sender.nickname}：`
      : "";
  return `${prefix}${message.body.replace(/\s+/g, " ").trim()}`;
}

function formatMessageTime(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "" : messageTimeFormatter.format(date);
}

function formatConversationTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  if (date.toDateString() === new Date().toDateString()) {
    return messageTimeFormatter.format(date);
  }
  return conversationTimeFormatter.format(date);
}

function formatDay(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  const today = new Date();
  if (date.toDateString() === today.toDateString()) {
    return "今天";
  }
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);
  if (date.toDateString() === yesterday.toDateString()) {
    return "昨天";
  }
  return dayFormatter.format(date);
}

function isSameDay(left: string, right: string): boolean {
  const leftDate = new Date(left);
  const rightDate = new Date(right);
  if (Number.isNaN(leftDate.getTime()) || Number.isNaN(rightDate.getTime())) {
    return true;
  }
  return leftDate.toDateString() === rightDate.toDateString();
}

function parseIncomingMessage(value: unknown): ChatMessage | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const event = value as { type?: unknown; message?: unknown };
  if (event.type !== "message.created" || !event.message || typeof event.message !== "object") {
    return null;
  }
  const candidate = event.message as Partial<ChatMessage>;
  if (
    typeof candidate.id !== "number"
    || typeof candidate.body !== "string"
    || typeof candidate.client_message_id !== "string"
    || typeof candidate.created_at !== "string"
    || !candidate.sender
    || typeof candidate.sender.username !== "string"
    || typeof candidate.sender.nickname !== "string"
    || !candidate.recipient
    || typeof candidate.recipient.username !== "string"
    || typeof candidate.recipient.nickname !== "string"
  ) {
    return null;
  }
  return candidate as ChatMessage;
}

function parseGroupIncomingMessage(value: unknown): GroupMessage | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const event = value as { type?: unknown; message?: unknown };
  if (
    event.type !== "group.message.created"
    || !event.message
    || typeof event.message !== "object"
  ) {
    return null;
  }
  const candidate = event.message as Partial<GroupMessage>;
  if (
    typeof candidate.id !== "number"
    || typeof candidate.group_id !== "number"
    || typeof candidate.body !== "string"
    || typeof candidate.client_message_id !== "string"
    || typeof candidate.created_at !== "string"
    || !candidate.sender
    || typeof candidate.sender.username !== "string"
    || typeof candidate.sender.nickname !== "string"
  ) {
    return null;
  }
  return candidate as GroupMessage;
}

function parseReadEvent(value: unknown): ReadEvent | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const event = value as {
    type?: unknown;
    reader?: unknown;
    peer?: unknown;
    last_read_message_id?: unknown;
  };
  if (
    event.type !== "messages.read"
    || typeof event.reader !== "string"
    || typeof event.peer !== "string"
    || typeof event.last_read_message_id !== "number"
  ) {
    return null;
  }
  return {
    reader: event.reader,
    peer: event.peer,
    lastReadMessageID: event.last_read_message_id,
  };
}

function parseGroupChangedEvent(value: unknown): GroupChangedEvent | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const event = value as {
    type?: unknown;
    group_id?: unknown;
    action?: unknown;
  };
  if (
    event.type !== "group.changed"
    || typeof event.group_id !== "number"
    || typeof event.action !== "string"
  ) {
    return null;
  }
  return { groupID: event.group_id, action: event.action };
}

async function fetchInitialPage(
  target: ActiveTarget,
): Promise<{ messages: DisplayMessage[]; hasMore: boolean }> {
  if (target.kind === "direct") {
    const page = await api.listMessages(target.username, undefined, PAGE_SIZE);
    return {
      messages: page.messages.map(toDirectDisplay),
      hasMore: page.has_more,
    };
  }
  const page = await api.listGroupMessages(target.groupID, undefined, PAGE_SIZE);
  return {
    messages: page.messages.map(toGroupDisplay),
    hasMore: page.has_more,
  };
}

async function fetchOlderPage(
  target: ActiveTarget,
  beforeID: number,
): Promise<{ messages: DisplayMessage[]; hasMore: boolean }> {
  if (target.kind === "direct") {
    const page = await api.listMessages(target.username, beforeID, PAGE_SIZE);
    return {
      messages: page.messages.map(toDirectDisplay),
      hasMore: page.has_more,
    };
  }
  const page = await api.listGroupMessages(target.groupID, beforeID, PAGE_SIZE);
  return {
    messages: page.messages.map(toGroupDisplay),
    hasMore: page.has_more,
  };
}

async function fetchAfterPage(
  target: ActiveTarget,
  afterID: number,
): Promise<{ messages: DisplayMessage[]; hasMore: boolean }> {
  if (target.kind === "direct") {
    const page = await api.listMessagesAfter(target.username, afterID, 50);
    return {
      messages: page.messages.map(toDirectDisplay),
      hasMore: page.has_more,
    };
  }
  const page = await api.listGroupMessagesAfter(target.groupID, afterID, 50);
  return {
    messages: page.messages.map(toGroupDisplay),
    hasMore: page.has_more,
  };
}

export function ChatPage() {
  const { user, logout, invalidate, bootstrap, status } = useAuth();
  const coldOffline = status === "offline";
  const [networkOnline, setNetworkOnline] = useState(() => navigator.onLine);
  const offline = coldOffline || !networkOnline;
  const offlineRef = useRef(offline);
  const bootConversations = bootstrap !== null
    && !bootstrap.partial.includes("conversations")
    ? bootstrap.conversations
    : null;
  const bootFriends = bootstrap !== null
    && !bootstrap.partial.includes("friends")
    ? bootstrap.friends
    : null;
  const routeNavigate = useNavigate();
  // Route changes must not restart workspace loading or its WebSocket.
  const navigateRef = useRef(routeNavigate);
  useLayoutEffect(() => {
    navigateRef.current = routeNavigate;
  }, [routeNavigate]);
  const navigate = useCallback((to: To, options?: NavigateOptions) =>
    navigateRef.current(to, options), []);
  const params = useParams<{ username?: string; groupId?: string }>();
  const routeGroupIDText = params.groupId ?? "";
  const routeGroupID = Number(routeGroupIDText);
  const routeUsername = (params.username ?? "").trim();
  const invalidGroupRoute = routeGroupIDText !== ""
    && (!Number.isInteger(routeGroupID) || routeGroupID <= 0);
  const activeTarget: ActiveTarget | null = routeGroupIDText !== ""
    ? (invalidGroupRoute
      ? null
      : { kind: "group", key: groupKey(routeGroupID), groupID: routeGroupID })
    : routeUsername
      ? { kind: "direct", key: directKey(routeUsername), username: routeUsername }
      : null;
  const activeKey = activeTarget?.key ?? "";
  const activeUsername = activeTarget?.kind === "direct" ? activeTarget.username : "";
  const activeGroupID = activeTarget?.kind === "group" ? activeTarget.groupID : 0;

  const [conversations, setConversations] = useState<Conversation[]>(
    () => bootConversations ?? [],
  );
  const [conversationsLoading, setConversationsLoading] = useState(
    bootConversations === null,
  );
  const [conversationsError, setConversationsError] = useState<string | null>(null);
  const [messages, setMessages] = useState<DisplayMessage[]>([]);
  const [messagesKey, setMessagesKey] = useState("");
  const [messagesLoading, setMessagesLoading] = useState(false);
  const [messagesError, setMessagesError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [loadingOlder, setLoadingOlder] = useState(false);
  const [draft, setDraft] = useState("");
  const [draftError, setDraftError] = useState<string | null>(null);
  const [sendError, setSendError] = useState<string | null>(null);
  const [notFriendLocally, setNotFriendLocally] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>("connecting");
  const [reconnectAttempt, setReconnectAttempt] = useState(0);
  const [pageVisible, setPageVisible] = useState(() => document.visibilityState === "visible");
  const [loggingOut, setLoggingOut] = useState(false);
  const [chatNotice, setChatNotice] = useState<string | null>(null);
  const [groupDetail, setGroupDetail] = useState<GroupDetail | null>(null);
  const [groupDetailLoading, setGroupDetailLoading] = useState(false);
  const [groupDetailError, setGroupDetailError] = useState<string | null>(null);
  const [groupReceipt, setGroupReceipt] = useState<GroupReceipt | null>(null);
  const [groupInfoOpen, setGroupInfoOpen] = useState(false);
  const [groupNameDraft, setGroupNameDraft] = useState("");
  const [groupActionError, setGroupActionError] = useState<string | null>(null);
  const [groupActionBusy, setGroupActionBusy] = useState(false);
  const [confirmLeave, setConfirmLeave] = useState(false);
  const [inviteSelection, setInviteSelection] = useState<string[]>([]);
  const [createGroupOpen, setCreateGroupOpen] = useState(false);
  const [createGroupName, setCreateGroupName] = useState("");
  const [createGroupSelection, setCreateGroupSelection] = useState<string[]>([]);
  const [createGroupError, setCreateGroupError] = useState<string | null>(null);
  const [createGroupBusy, setCreateGroupBusy] = useState(false);
  const [friends, setFriends] = useState<PublicUser[]>(() => bootFriends ?? []);
  const [friendsLoading, setFriendsLoading] = useState(false);
  const [friendsError, setFriendsError] = useState<string | null>(null);

  const activeTargetRef = useRef<ActiveTarget | null>(activeTarget);
  const userRef = useRef(user);
  const conversationKeysRef = useRef<Set<string>>(new Set());
  const conversationLastIDsRef = useRef<Map<string, number>>(new Map());
  const messageListRef = useRef<HTMLDivElement>(null);
  const composerRef = useRef<HTMLTextAreaElement>(null);
  const websocketRef = useRef<WebSocket | null>(null);
  const loggingOutRef = useRef(false);
  const messagesRef = useRef<DisplayMessage[]>([]);
  const pageVisibleRef = useRef(pageVisible);
  const replacedRef = useRef(false);
  const mountedRef = useRef(true);
  const skipInitialConversationsRef = useRef(bootConversations !== null);
  const skipSocketConversationRefreshRef = useRef(bootConversations !== null);
  const skipInitialSocketSyncRef = useRef(bootConversations !== null);
  const workspaceStartedAtRef = useRef(Date.now());
  const reconnectTimerRef = useRef<number | null>(null);
  const reconnectAttemptRef = useRef(0);
  const connectRef = useRef<() => void>(() => {});
  const syncingRef = useRef(false);
  const groupReceiptTargetRef = useRef<{ groupID: number; messageID: number } | null>(null);
  const readStatsGateRef = useRef(new ReadStatsGate());
  const readStatsDebouncerRef = useRef(new Debouncer(READ_STATS_DEBOUNCE_MS));
  const nearBottomRef = useRef(true);
  const scrollToBottomNextRef = useRef(false);
  const pendingOlderScrollRef = useRef<ScrollPosition | null>(null);
  const messagesRequestRef = useRef(0);
  const conversationVersionRef = useRef(0);
  const conversationRequestRef = useRef(0);
  const groupDetailRequestRef = useRef(0);
  const friendsRequestRef = useRef(0);

  useEffect(() => {
    offlineRef.current = offline;
  }, [offline]);

  useEffect(() => {
    const update = () => setNetworkOnline(navigator.onLine);
    window.addEventListener("online", update);
    window.addEventListener("offline", update);
    return () => {
      window.removeEventListener("online", update);
      window.removeEventListener("offline", update);
    };
  }, []);

  useEffect(() => {
    activeTargetRef.current = activeTarget;
  }, [activeKey, activeUsername, activeGroupID]);

  useEffect(() => {
    userRef.current = user;
  }, [user]);

  useEffect(() => {
    conversationKeysRef.current = new Set(conversations.map(conversationKey));
    const lastIDs = new Map<string, number>();
    for (const conversation of conversations) {
      lastIDs.set(conversationKey(conversation), conversation.last_message?.id ?? 0);
    }
    conversationLastIDsRef.current = lastIDs;
  }, [conversations]);

  useEffect(() => {
    messagesRef.current = messages;
  }, [messages]);

  useEffect(() => {
    pageVisibleRef.current = pageVisible;
  }, [pageVisible]);

  const handleSessionError = useCallback((error: unknown): boolean => {
    if (!isSessionError(error)) {
      return false;
    }
    invalidate(error.code);
    navigate("/login", { replace: true });
    return true;
  }, [invalidate, navigate]);

  const isCurrentConversation = useCallback((key: string, version: number) =>
    activeTargetRef.current?.key === key && conversationVersionRef.current === version,
  []);

  const loseGroupAccess = useCallback((groupID: number, notice: string) => {
    conversationRequestRef.current += 1;
    setConversationsLoading(false);
    setConversations((previous) => previous.filter((conversation) =>
      !(conversation.type === "group" && conversation.group.id === groupID)));
    const target = activeTargetRef.current;
    if (target?.kind !== "group" || target.groupID !== groupID) {
      return;
    }
    activeTargetRef.current = null;
    conversationVersionRef.current += 1;
    messagesRequestRef.current += 1;
    groupDetailRequestRef.current += 1;
    setGroupDetail(null);
    setGroupDetailError(null);
    setGroupDetailLoading(false);
    setGroupInfoOpen(false);
    setMessages([]);
    setMessagesKey("");
    setMessagesError(null);
    setMessagesLoading(false);
    setDraft("");
    setChatNotice(notice);
    navigate("/", { replace: true });
  }, [navigate]);

  const clearLocalUnread = useCallback((target: FocusTarget, throughID: number) => {
    setConversations((previous) => previous.map((conversation) => {
      if (target.kind === "direct") {
        return conversation.type === "direct"
          && sameUsername(conversation.user.username, target.username)
          && (conversation.last_message?.id ?? 0) <= throughID
          ? { ...conversation, unread_count: 0 }
          : conversation;
      }
      return conversation.type === "group"
        && conversation.group.id === target.groupID
        && (conversation.last_message?.id ?? 0) <= throughID
        ? { ...conversation, unread_count: 0 }
        : conversation;
    }));
  }, []);

  const syncFocus = useCallback(() => {
    const socket = websocketRef.current;
    if (socket === null || socket.readyState !== WebSocket.OPEN) {
      return;
    }
    const target = activeTargetRef.current;
    const frame = target !== null && pageVisibleRef.current
      ? focusFrame(target)
      : BLUR_FRAME;
    socket.send(JSON.stringify(frame));
  }, []);

  const handleFocusAck = useCallback((ack: FocusAck) => {
    if (!ack.ok) {
      return;
    }
    clearLocalUnread(ack.target, ack.lastReadMessageID);
  }, [clearLocalUnread]);

  const fetchGroupReadStats = useCallback(async () => {
    const target = groupReceiptTargetRef.current;
    if (target === null || offlineRef.current) {
      return;
    }
    const token = readStatsGateRef.current.begin(
      target.groupID,
      target.messageID,
    );
    try {
      const stats = parseGroupReadStats(
        await api.groupReadStats(target.groupID, target.messageID),
      );
      if (
        stats === null
        || !readStatsGateRef.current.accepts(
          token,
          target.groupID,
          target.messageID,
        )
      ) {
        return;
      }
      setGroupReceipt({
        groupID: target.groupID,
        messageID: stats.message_id,
        stats,
      });
    } catch (error) {
      // Keep the last good value; the next trigger retries.
      handleSessionError(error);
    }
  }, [handleSessionError]);

  const handleGroupRead = useCallback((event: GroupReadEvent) => {
    const target = activeTargetRef.current;
    if (target?.kind !== "group" || target.groupID !== event.groupID) {
      return;
    }
    if (groupReceiptTargetRef.current === null) {
      return;
    }
    readStatsDebouncerRef.current.schedule(() => {
      void fetchGroupReadStats();
    });
  }, [fetchGroupReadStats]);

  useEffect(() => () => {
    readStatsDebouncerRef.current.cancel();
  }, []);

  const loadConversations = useCallback(async (initial = false) => {
    const requestID = ++conversationRequestRef.current;
    if (initial) {
      setConversationsLoading(true);
    }
    try {
      const response = await api.listConversations();
      if (requestID !== conversationRequestRef.current) {
        return;
      }
      setConversations(sortConversations(response.conversations));
      setConversationsError(null);
    } catch (error) {
      if (!handleSessionError(error) && requestID === conversationRequestRef.current) {
        setConversationsError(describeError(error));
      }
    } finally {
      if (requestID === conversationRequestRef.current) {
        setConversationsLoading(false);
      }
    }
  }, [handleSessionError]);

  const loadFriends = useCallback(async () => {
    if (offline) {
      return;
    }
    const requestID = ++friendsRequestRef.current;
    setFriendsLoading(true);
    try {
      const response = await api.listFriends();
      if (requestID !== friendsRequestRef.current) {
        return;
      }
      setFriends(response.friends);
      setFriendsError(null);
      if (user) {
        void writeFriends(user.username, response.friends);
      }
    } catch (error) {
      if (requestID !== friendsRequestRef.current) {
        return;
      }
      if (!handleSessionError(error)) {
        setFriendsError(describeError(error));
      }
    } finally {
      if (requestID === friendsRequestRef.current) {
        setFriendsLoading(false);
      }
    }
  }, [handleSessionError, offline, user]);

  const loadGroupDetail = useCallback(async (groupID: number, showLoading = false) => {
    if (offline) {
      return;
    }
    const version = conversationVersionRef.current;
    const requestID = ++groupDetailRequestRef.current;
    const isCurrent = () => requestID === groupDetailRequestRef.current
      && isCurrentConversation(groupKey(groupID), version);
    if (showLoading) {
      setGroupDetailLoading(true);
    }
    try {
      const detail = await api.getGroup(groupID);
      if (!isCurrent()) {
        return;
      }
      setGroupDetail(detail);
      setGroupDetailError(null);
      setGroupNameDraft(detail.group.name);
    } catch (error) {
      if (handleSessionError(error) || !isCurrent()) {
        return;
      }
      if (error instanceof ApiError && error.status === 404 && error.code === "group_not_found") {
        loseGroupAccess(groupID, "群聊已解散或你已不在群中。");
      } else {
        setGroupDetailError(describeError(error));
      }
    } finally {
      if (requestID === groupDetailRequestRef.current) {
        setGroupDetailLoading(false);
      }
    }
  }, [handleSessionError, isCurrentConversation, loseGroupAccess, offline]);

  const loadInitialMessages = useCallback(async (target: ActiveTarget) => {
    if (activeTargetRef.current?.key !== target.key) {
      return;
    }
    const version = conversationVersionRef.current;
    const requestID = ++messagesRequestRef.current;
    const isCurrent = () => requestID === messagesRequestRef.current
      && isCurrentConversation(target.key, version);
    setMessagesKey(target.key);
    setMessages([]);
    setMessagesLoading(true);
    setMessagesError(null);
    setHasMore(false);
    setLoadingOlder(false);
    setDraftError(null);
    setSendError(null);
    setNotFriendLocally(false);
    nearBottomRef.current = true;
    pendingOlderScrollRef.current = null;

    const username = user?.username ?? "";
    let cached: CachedMessage[] = [];
    let directComplete = true;
    if (username !== "") {
      const [stored, syncState] = await Promise.all([
        readMessages(username, target.key),
        readSyncState(username),
      ]);
      cached = stored;
      directComplete = syncState.directComplete;
    }
    if (!isCurrent()) {
      return;
    }
    const staleDirect = target.kind === "direct" && !directComplete;
    if (staleDirect && cached.length > 0 && username !== "") {
      // The global feed had a gap, so this conversation's cache cannot be
      // stitched. Reload a fresh page instead.
      void removeMessages(username, target.key);
      cached = [];
    }
    if (cached.length > 0) {
      setMessages(cached.map(toDisplayMessage));
      setMessagesLoading(false);
      setHasMore(true);
      scrollToBottomNextRef.current = true;
    }
    if (offlineRef.current) {
      setMessagesLoading(false);
      if (cached.length === 0) {
        setMessagesError("当前离线，这个会话没有缓存。");
      }
      return;
    }

    try {
      const newest = cached.reduce(
        (latest, message) => Math.max(latest, message.id),
        0,
      );
      const knownLast = conversationLastIDsRef.current.get(target.key) ?? 0;
      if (newest > 0 && knownLast > 0 && knownLast <= newest) {
        if (nearBottomRef.current) {
          scrollToBottomNextRef.current = true;
        }
      } else if (newest === 0) {
        const page = await fetchInitialPage(target);
        if (!isCurrent()) {
          return;
        }
        setMessages(page.messages);
        setHasMore(page.hasMore);
        scrollToBottomNextRef.current = true;
        if (username !== "") {
          void writeMessages(username, target.key, page.messages);
        }
      } else {
        let after = newest;
        for (let round = 0; round < 10; round += 1) {
          const page = await fetchAfterPage(target, after);
          if (!isCurrent()) {
            return;
          }
          if (page.messages.length === 0) {
            break;
          }
          setMessages((previous) => mergeMessages(previous, page.messages));
          if (username !== "") {
            void writeMessages(username, target.key, page.messages);
          }
          const nextAfter = page.messages[page.messages.length - 1].id;
          if (nextAfter <= after) {
            break;
          }
          after = nextAfter;
          if (!page.hasMore) {
            break;
          }
        }
        if (nearBottomRef.current) {
          scrollToBottomNextRef.current = true;
        }
      }
    } catch (error) {
      if (handleSessionError(error) || !isCurrent()) {
        return;
      }
      if (target.kind === "group" && error instanceof ApiError
        && error.status === 404 && error.code === "group_not_found") {
        loseGroupAccess(target.groupID, "群聊已解散或你已不在群中。");
      } else if (cached.length === 0) {
        setMessagesError(describeError(error));
      }
    } finally {
      if (isCurrent()) {
        setMessagesLoading(false);
      }
    }
  }, [
    handleSessionError,
    isCurrentConversation,
    loseGroupAccess,
    user?.username,
  ]);

  useEffect(() => {
    if (skipInitialConversationsRef.current) {
      skipInitialConversationsRef.current = false;
      return;
    }
    if (offline) {
      setConversationsLoading(false);
      return;
    }
    void loadConversations(true);
  }, [loadConversations, offline]);

  useEffect(() => {
    if (!user || bootstrap !== null) {
      return;
    }
    let cancelled = false;
    void (async () => {
      const [cachedConversations, cachedFriends] = await Promise.all([
        readConversations(user.username),
        readFriends(user.username),
      ]);
      if (cancelled) {
        return;
      }
      setConversations(sortConversations(cachedConversations));
      setConversationsLoading(false);
      setFriends(cachedFriends);
    })();
    return () => {
      cancelled = true;
    };
  }, [bootstrap, user]);

  useEffect(() => {
    if (!user || conversationsLoading || offline) {
      return;
    }
    const timer = window.setTimeout(() => {
      void writeConversations(user.username, conversations);
    }, 300);
    return () => window.clearTimeout(timer);
  }, [conversations, conversationsLoading, offline, user]);

  useEffect(() => {
    if (groupDetail) {
      setGroupNameDraft(groupDetail.group.name);
    }
  }, [groupDetail]);

  useEffect(() => {
    if (createGroupOpen || groupInfoOpen) {
      void loadFriends();
    }
  }, [createGroupOpen, groupInfoOpen, loadFriends]);

  useEffect(() => {
    conversationVersionRef.current += 1;
    setDraft("");
    setDraftError(null);
    setSendError(null);
    setGroupInfoOpen(false);
    setGroupActionError(null);
    setConfirmLeave(false);
    setInviteSelection([]);
    const target = activeTargetRef.current;
    if (!target) {
      messagesRequestRef.current += 1;
      setMessages([]);
      setMessagesKey("");
      setMessagesError(null);
      setMessagesLoading(false);
      setHasMore(false);
      setLoadingOlder(false);
      setNotFriendLocally(false);
      return;
    }
    void loadInitialMessages(target);
  }, [activeKey, activeUsername, activeGroupID, loadInitialMessages]);

  useEffect(() => {
    groupDetailRequestRef.current += 1;
    setGroupDetail(null);
    setGroupDetailError(null);
    setGroupNameDraft("");
    if (activeGroupID) {
      void loadGroupDetail(activeGroupID, true);
    } else {
      setGroupDetailLoading(false);
      setGroupInfoOpen(false);
    }
  }, [activeGroupID, loadGroupDetail]);

  const activeDirect = useMemo(
    () => activeTarget?.kind === "direct"
      ? conversations.find(
        (conversation): conversation is DirectConversation =>
          conversation.type === "direct"
          && conversation.user.username.toLocaleLowerCase()
            === activeTarget.username.toLocaleLowerCase(),
      )
      : undefined,
    [conversations, activeKey, activeUsername],
  );
  const activeGroup = useMemo(
    () => activeTarget?.kind === "group"
      ? conversations.find(
        (conversation): conversation is GroupConversation =>
          conversation.type === "group" && conversation.group.id === activeTarget.groupID,
      )
      : undefined,
    [conversations, activeKey, activeGroupID],
  );
  const activeMissing = Boolean(activeTarget)
    && !conversationsLoading
    && !activeDirect
    && !activeGroup;
  const isReadOnly = Boolean(activeDirect && !activeDirect.is_friend)
    || notFriendLocally;
  const isReplaced = connectionStatus === "replaced";
  const activeUser = activeDirect?.user;
  const messagesBelongToActive = activeKey !== ""
    && messagesKey === activeKey;
  const peerLastReadMessageID = activeDirect?.peer_last_read_message_id ?? 0;
  const lastOwnSentMessageID = useMemo(() => {
    if (!user || !messagesBelongToActive) {
      return 0;
    }
    return findLastOwnSentMessageID(messages, user.username);
  }, [messages, messagesBelongToActive, user]);

  useEffect(() => {
    syncFocus();
  }, [activeKey, syncFocus]);

  useEffect(() => {
    syncFocus();
  }, [pageVisible, syncFocus]);

  useEffect(() => {
    readStatsDebouncerRef.current.cancel();
    readStatsGateRef.current.reset();
    const target = activeGroupID > 0 && messagesBelongToActive
      && lastOwnSentMessageID > 0
      ? { groupID: activeGroupID, messageID: lastOwnSentMessageID }
      : null;
    groupReceiptTargetRef.current = target;
    setGroupReceipt(null);
    if (target !== null) {
      void fetchGroupReadStats();
    }
  }, [
    activeGroupID,
    fetchGroupReadStats,
    lastOwnSentMessageID,
    messagesBelongToActive,
  ]);

  useEffect(() => {
    if (!pageVisible) {
      return;
    }
    void fetchGroupReadStats();
  }, [fetchGroupReadStats, pageVisible]);

  const loadOlderMessages = useCallback(async () => {
    const target = activeTargetRef.current;
    if (!target || !hasMore || loadingOlder || messagesLoading) {
      return;
    }
    const oldest = messages.find((message) => message.id > 0);
    if (!oldest) {
      return;
    }
    const version = conversationVersionRef.current;
    const container = messageListRef.current;
    const previousHeight = container?.scrollHeight ?? 0;
    const previousTop = container?.scrollTop ?? 0;
    setLoadingOlder(true);
    try {
      const page = await fetchOlderPage(target, oldest.id);
      if (version !== conversationVersionRef.current) {
        return;
      }
      pendingOlderScrollRef.current = { height: previousHeight, top: previousTop };
      setMessages((previous) => mergeMessages(previous, page.messages));
      if (user) {
        void writeMessages(user.username, target.key, page.messages);
      }
      setHasMore(page.hasMore);
      setMessagesError(null);
    } catch (error) {
      if (version !== conversationVersionRef.current) {
        return;
      }
      if (!handleSessionError(error)) {
        setMessagesError(describeError(error));
      }
    } finally {
      if (version === conversationVersionRef.current) {
        setLoadingOlder(false);
      }
    }
  }, [
    handleSessionError,
    hasMore,
    loadingOlder,
    messages,
    messagesLoading,
    user,
  ]);

  const handleScroll = useCallback(() => {
    const container = messageListRef.current;
    if (!container) {
      return;
    }
    nearBottomRef.current = container.scrollHeight
      - container.scrollTop
      - container.clientHeight < 100;
    if (container.scrollTop < 80 && hasMore && !loadingOlder && !messagesLoading) {
      void loadOlderMessages();
    }
  }, [hasMore, loadingOlder, loadOlderMessages, messagesLoading]);

  useLayoutEffect(() => {
    const container = messageListRef.current;
    if (!container) {
      return;
    }
    const pending = pendingOlderScrollRef.current;
    if (pending) {
      container.scrollTop = container.scrollHeight - pending.height + pending.top;
      pendingOlderScrollRef.current = null;
      return;
    }
    if (scrollToBottomNextRef.current) {
      scrollToBottomNextRef.current = false;
      container.scrollTop = container.scrollHeight;
    }
  }, [messages]);

  const handleIncomingMessage = useCallback((message: ChatMessage) => {
    const currentUser = userRef.current;
    if (!currentUser) {
      return;
    }
    const sentByCurrent = sameUsername(message.sender.username, currentUser.username);
    const receivedByCurrent = sameUsername(message.recipient.username, currentUser.username);
    if (!sentByCurrent && !receivedByCurrent) {
      return;
    }
    const otherUser = sentByCurrent ? message.recipient : message.sender;
    const incoming = receivedByCurrent;
    void writeMessages(
      currentUser.username,
      directKey(otherUser.username),
      [message],
    );
    const target = activeTargetRef.current;
    const targetMatches = target?.kind === "direct"
      && sameUsername(target.username, otherUser.username);
    const readingNow = targetMatches
      && pageVisibleRef.current
      && !replacedRef.current;

    if (conversationKeysRef.current.has(directKey(otherUser.username))) {
      setConversations((previous) => sortConversations(previous.map((conversation) =>
        conversation.type === "direct"
          && sameUsername(conversation.user.username, otherUser.username)
          ? {
            ...conversation,
            last_message: message,
            unread_count: incoming
              ? (readingNow ? 0 : conversation.unread_count + 1)
              : conversation.unread_count,
          }
          : conversation)));
    } else {
      void loadConversations(false);
    }

    if (targetMatches) {
      const wasNearBottom = nearBottomRef.current;
      setMessages((previous) => mergeMessages(previous, [toDirectDisplay(message)]));
      if (wasNearBottom) {
        scrollToBottomNextRef.current = true;
      }
    }
  }, [loadConversations]);

  const handleIncomingGroupMessage = useCallback((message: GroupMessage) => {
    const currentUser = userRef.current;
    if (!currentUser) {
      return;
    }
    const sentByCurrent = sameUsername(message.sender.username, currentUser.username);
    const target = activeTargetRef.current;
    const readingNow = target?.kind === "group"
      && target.groupID === message.group_id
      && pageVisibleRef.current
      && !replacedRef.current;
    void writeMessages(
      currentUser.username,
      groupKey(message.group_id),
      [message],
    );

    if (conversationKeysRef.current.has(groupKey(message.group_id))) {
      setConversations((previous) => sortConversations(previous.map((conversation) =>
        conversation.type === "group" && conversation.group.id === message.group_id
          ? {
            ...conversation,
            last_message: message,
            unread_count: sentByCurrent
              ? conversation.unread_count
              : (readingNow ? 0 : conversation.unread_count + 1),
          }
          : conversation)));
    } else {
      void loadConversations(false);
    }

    if (target?.kind === "group" && target.groupID === message.group_id) {
      const wasNearBottom = nearBottomRef.current;
      setMessages((previous) => mergeMessages(previous, [toGroupDisplay(message)]));
      if (wasNearBottom) {
        scrollToBottomNextRef.current = true;
      }
    }
  }, [loadConversations]);

  const handleReadEvent = useCallback((event: ReadEvent) => {
    const currentUser = userRef.current;
    if (!currentUser) {
      return;
    }
    if (sameUsername(event.reader, currentUser.username)) {
      // The focus ack already cleared local unread; do not refetch the
      // whole conversation list for our own read echo.
      setConversations((previous) => previous.map((conversation) =>
        conversation.type === "direct"
          && sameUsername(conversation.user.username, event.peer)
          && (conversation.last_message?.id ?? 0) <= event.lastReadMessageID
          ? { ...conversation, unread_count: 0 }
          : conversation));
      return;
    }
    if (sameUsername(event.peer, currentUser.username)) {
      setConversations((previous) => previous.map((conversation) =>
        conversation.type === "direct"
          && sameUsername(conversation.user.username, event.reader)
          ? {
            ...conversation,
            peer_last_read_message_id: Math.max(
              conversation.peer_last_read_message_id ?? 0,
              event.lastReadMessageID,
            ),
          }
          : conversation));
    }
  }, []);

  const handleGroupChanged = useCallback((event: GroupChangedEvent) => {
    if (event.action === "removed" || event.action === "dissolved") {
      loseGroupAccess(event.groupID, event.action === "dissolved"
        ? "该群聊已解散。"
        : "你已被移出群聊。");
      return;
    }
    void loadConversations(false);
    if (
      activeTargetRef.current?.kind === "group"
      && activeTargetRef.current.groupID === event.groupID
    ) {
      void loadGroupDetail(event.groupID);
      void fetchGroupReadStats();
    }
  }, [fetchGroupReadStats, loadConversations, loadGroupDetail, loseGroupAccess]);

  const scheduleReconnect = useCallback(() => {
    if (!mountedRef.current || replacedRef.current) {
      return;
    }
    if (reconnectTimerRef.current !== null) {
      return;
    }
    if (reconnectAttemptRef.current >= MAX_RECONNECT_ATTEMPTS) {
      return;
    }
    if (document.visibilityState !== "visible" || !navigator.onLine) {
      return;
    }

    const attempt = reconnectAttemptRef.current + 1;
    reconnectAttemptRef.current = attempt;
    setReconnectAttempt(attempt);
    const delay = Math.min(
      RECONNECT_BASE_DELAY * 2 ** (attempt - 1),
      RECONNECT_MAX_DELAY,
    );
    reconnectTimerRef.current = window.setTimeout(() => {
      reconnectTimerRef.current = null;
      connectRef.current();
    }, delay);
  }, []);

  const syncActiveMessages = useCallback(async (target: ActiveTarget) => {
    const version = conversationVersionRef.current;
    const isCurrent = () => isCurrentConversation(target.key, version);
    if (syncingRef.current || !isCurrent()) {
      return;
    }
    const newestID = messagesRef.current.reduce((newest, message) => {
      const belongs = target.kind === "group"
        ? isGroupMessage(message) && message.group_id === target.groupID
        : !isGroupMessage(message)
          && (message.sender.username.toLocaleLowerCase() === target.username.toLocaleLowerCase()
            || message.recipient.username.toLocaleLowerCase() === target.username.toLocaleLowerCase());
      return belongs ? Math.max(newest, message.id) : newest;
    }, 0);

    syncingRef.current = true;
    try {
      if (newestID === 0) {
        const page = await fetchInitialPage(target);
        if (!isCurrent()) {
          return;
        }
        setMessages((previous) => mergeMessages(previous, page.messages));
        setHasMore(page.hasMore);
        setMessagesError(null);
        if (user) {
          void writeMessages(user.username, target.key, page.messages);
        }
      } else {
        let afterID = newestID;
        let more = true;
        for (let pageIndex = 0; pageIndex < 10 && more; pageIndex += 1) {
          const page = await fetchAfterPage(target, afterID);
          if (!isCurrent()) {
            return;
          }
          if (page.messages.length > 0) {
            setMessages((previous) => mergeMessages(previous, page.messages));
            if (user) {
              void writeMessages(user.username, target.key, page.messages);
            }
            afterID = page.messages[page.messages.length - 1].id;
          }
          more = page.hasMore;
        }
        if (more && isCurrent()) {
          await loadInitialMessages(target);
          return;
        }
      }
      if (isCurrent() && nearBottomRef.current) {
        scrollToBottomNextRef.current = true;
      }
    } catch (error) {
      if (handleSessionError(error) || !isCurrent()) {
        return;
      }
      if (target.kind === "group" && error instanceof ApiError
        && error.status === 404 && error.code === "group_not_found") {
        loseGroupAccess(target.groupID, "群聊已解散或你已不在群中。");
      } else {
        await loadInitialMessages(target);
      }
    } finally {
      syncingRef.current = false;
    }
  }, [
    handleSessionError,
    isCurrentConversation,
    loadInitialMessages,
    loseGroupAccess,
    user,
  ]);

  const connectWebSocket = useCallback((resetAttempts = false) => {
    if (resetAttempts) {
      reconnectAttemptRef.current = 0;
      setReconnectAttempt(0);
    }
    if (reconnectTimerRef.current !== null) {
      window.clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
    const existing = websocketRef.current;
    if (existing && (existing.readyState === WebSocket.CONNECTING
      || existing.readyState === WebSocket.OPEN)) {
      return;
    }

    setConnectionStatus("connecting");
    const socket = new WebSocket(websocketURL());
    websocketRef.current = socket;

    socket.onopen = () => {
      if (websocketRef.current !== socket) {
        return;
      }
      replacedRef.current = false;
      reconnectAttemptRef.current = 0;
      setReconnectAttempt(0);
      setConnectionStatus("open");
      if (skipSocketConversationRefreshRef.current) {
        skipSocketConversationRefreshRef.current = false;
      } else {
        void loadConversations(false);
      }
      const target = activeTargetRef.current;
      const skipInitialSync = skipInitialSocketSyncRef.current;
      skipInitialSocketSyncRef.current = false;
      syncFocus();
      void fetchGroupReadStats();
      if (target && !skipInitialSync) {
        if (target.kind === "group") {
          void loadGroupDetail(target.groupID);
        }
        void syncActiveMessages(target);
      }
    };
    socket.onmessage = (event) => {
      try {
        const data = JSON.parse(String(event.data));
        if (data?.type === "message.created") {
          const message = parseIncomingMessage(data);
          if (message) {
            handleIncomingMessage(message);
          }
        } else if (data?.type === "group.message.created") {
          const message = parseGroupIncomingMessage(data);
          if (message) {
            handleIncomingGroupMessage(message);
          }
        } else if (data?.type === "messages.read") {
          const readEvent = parseReadEvent(data);
          if (readEvent) {
            handleReadEvent(readEvent);
          }
        } else if (data?.type === "group.changed") {
          const groupEvent = parseGroupChangedEvent(data);
          if (groupEvent) {
            handleGroupChanged(groupEvent);
          }
        } else if (data?.type === "focus.ack") {
          const ack = parseFocusAck(data);
          if (ack) {
            handleFocusAck(ack);
          }
        } else if (data?.type === "group.read") {
          const readEvent = parseGroupReadEvent(data);
          if (readEvent) {
            handleGroupRead(readEvent);
          }
        }
      } catch {
        // Ignore malformed websocket events.
      }
    };
    socket.onclose = (event) => {
      if (websocketRef.current !== socket) {
        return;
      }
      websocketRef.current = null;
      if (event.code === WS_CLOSE_REPLACED) {
        replacedRef.current = true;
        reconnectAttemptRef.current = 0;
        setReconnectAttempt(0);
        setConnectionStatus("replaced");
        return;
      }
      if (event.code === WS_CLOSE_SESSION) {
        if (loggingOutRef.current) {
          navigate("/login", { replace: true });
          return;
        }
        invalidate(event.reason || "session_revoked");
        navigate("/login", { replace: true });
        return;
      }
      setConnectionStatus("closed");
      scheduleReconnect();
    };
    socket.onerror = () => {
      socket.close();
    };
  }, [
    fetchGroupReadStats,
    handleFocusAck,
    handleGroupChanged,
    handleGroupRead,
    handleIncomingGroupMessage,
    handleIncomingMessage,
    handleReadEvent,
    invalidate,
    loadConversations,
    loadGroupDetail,
    navigate,
    scheduleReconnect,
    syncActiveMessages,
    syncFocus,
  ]);

  useEffect(() => {
    connectRef.current = () => connectWebSocket(false);
  }, [connectWebSocket]);

  useEffect(() => {
    mountedRef.current = true;
    if (!offline) {
      connectWebSocket(true);
    }
    return () => {
      mountedRef.current = false;
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      const socket = websocketRef.current;
      websocketRef.current = null;
      if (socket) {
        socket.close();
      }
    };
  }, [connectWebSocket, offline]);

  useEffect(() => {
    if (offline) {
      setConnectionStatus("closed");
    }
  }, [offline]);

  useEffect(() => {
    const handleVisibility = () => {
      const visible = document.visibilityState === "visible";
      setPageVisible(visible);
      if (!visible || offline) {
        return;
      }
      // The bootstrap payload already covers the first second.
      if (Date.now() - workspaceStartedAtRef.current < 1000) {
        return;
      }
      void loadConversations(false);
      if (!replacedRef.current && websocketRef.current === null) {
        reconnectAttemptRef.current = 0;
        setReconnectAttempt(0);
        connectRef.current();
      }
    };
    const handleFocus = () => {
      if (document.visibilityState === "visible" && !offline
        && Date.now() - workspaceStartedAtRef.current >= 1000) {
        void loadConversations(false);
      }
    };
    const handleOnline = () => {
      if (coldOffline) {
        window.location.reload();
        return;
      }
      if (replacedRef.current || websocketRef.current !== null) {
        return;
      }
      reconnectAttemptRef.current = 0;
      setReconnectAttempt(0);
      connectRef.current();
    };
    document.addEventListener("visibilitychange", handleVisibility);
    window.addEventListener("focus", handleFocus);
    window.addEventListener("online", handleOnline);
    return () => {
      document.removeEventListener("visibilitychange", handleVisibility);
      window.removeEventListener("focus", handleFocus);
      window.removeEventListener("online", handleOnline);
    };
  }, [coldOffline, loadConversations, offline]);

  function manualReconnect() {
    connectWebSocket(true);
  }

  useEffect(() => {
    const element = composerRef.current;
    if (!element) {
      return;
    }
    element.style.height = "auto";
    element.style.height = `${Math.min(element.scrollHeight, 120)}px`;
  }, [draft]);

  async function deliverMessage(message: DisplayMessage, target: ActiveTarget) {
    const version = conversationVersionRef.current;
    const isCurrent = () => isCurrentConversation(target.key, version);
    try {
      const sent = target.kind === "direct"
        ? toDirectDisplay((await api.sendMessage(
          target.username,
          message.body,
          message.client_message_id,
        )).message)
        : toGroupDisplay((await api.sendGroupMessage(
          target.groupID,
          message.body,
          message.client_message_id,
        )).message);
      if (user) {
        void writeMessages(user.username, target.key, [sent]);
      }
      if (isCurrent()) {
        setMessages((previous) => mergeMessages(previous, [sent]));
        setSendError(null);
        composerRef.current?.focus();
      }
      if (conversationKeysRef.current.has(target.key)) {
        setConversations((previous) => sortConversations(previous.map((conversation) =>
          conversationKey(conversation) === target.key
            && (!conversation.last_message || conversation.last_message.id <= sent.id)
            ? { ...conversation, last_message: sent }
            : conversation)));
      } else {
        void loadConversations(false);
      }
    } catch (error) {
      if (handleSessionError(error)) {
        return;
      }
      if (
        target.kind === "direct"
        && error instanceof ApiError
        && error.code === "not_friends"
      ) {
        if (isCurrent()) {
          setNotFriendLocally(true);
        }
        setConversations((previous) => previous.map((conversation) =>
          conversation.type === "direct"
            && conversation.user.username.toLocaleLowerCase()
              === target.username.toLocaleLowerCase()
            ? { ...conversation, is_friend: false }
            : conversation));
      }
      if (isCurrent()) {
        setMessages((previous) => previous.map((item) =>
          item.client_message_id === message.client_message_id
            ? { ...item, status: "failed" }
            : item));
        setSendError(describeError(error));
      }
    }
  }

  async function submitMessage() {
    const target = activeTarget;
    if (!user || !target || isReadOnly || isReplaced || activeMissing || messagesLoading) {
      return;
    }
    if (
      (target.kind === "direct" && !activeDirect)
      || (target.kind === "group" && !activeGroup)
    ) {
      return;
    }
    const body = draft.trim();
    if (body === "") {
      return;
    }
    if ([...body].length > MAX_MESSAGE_LENGTH) {
      setDraftError(`消息不能超过 ${MAX_MESSAGE_LENGTH} 个字符。`);
      return;
    }

    const base = {
      id: 0,
      sender: { username: user.username, nickname: user.nickname },
      body,
      client_message_id: newClientMessageID(),
      created_at: new Date().toISOString(),
      status: "sending" as const,
    };
    const message: DisplayMessage = target.kind === "direct"
      ? { ...base, recipient: activeDirect!.user }
      : { ...base, group_id: target.groupID };
    setMessages((previous) => mergeMessages(previous, [message]));
    setDraft("");
    setDraftError(null);
    setSendError(null);
    scrollToBottomNextRef.current = true;
    await deliverMessage(message, target);
  }

  async function retryMessage(message: DisplayMessage) {
    const target = activeTarget;
    if (!target || isReadOnly || isReplaced) {
      return;
    }
    setMessages((previous) => previous.map((item) =>
      item.client_message_id === message.client_message_id
        ? { ...item, status: "sending" }
        : item));
    setSendError(null);
    await deliverMessage({ ...message, status: "sending" }, target);
  }

  function handleComposerKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
      event.preventDefault();
      void submitMessage();
    }
  }

  function handleComposerChange(value: string) {
    setDraft(value);
    setDraftError(null);
  }

  async function handleLogout() {
    if (loggingOut) {
      return;
    }
    loggingOutRef.current = true;
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

  function openCreateGroup() {
    setCreateGroupOpen(true);
    setCreateGroupName("");
    setCreateGroupSelection([]);
    setCreateGroupError(null);
  }

  function toggleCreateGroupSelection(username: string) {
    setCreateGroupSelection((previous) =>
      previous.includes(username)
        ? previous.filter((item) => item !== username)
        : [...previous, username]);
    setCreateGroupError(null);
  }

  async function submitCreateGroup() {
    const name = createGroupName.trim();
    if (!name || [...name].length > MAX_GROUP_NAME_LENGTH) {
      setCreateGroupError(`群名需要 1–${MAX_GROUP_NAME_LENGTH} 个字符。`);
      return;
    }
    if (createGroupSelection.length === 0) {
      setCreateGroupError("请至少选择 1 位好友。");
      return;
    }
    setCreateGroupBusy(true);
    setCreateGroupError(null);
    try {
      const detail = await api.createGroup(name, createGroupSelection);
      const conversation: GroupConversation = {
        type: "group",
        group: detail.group,
        is_owner: detail.is_owner,
        last_message: null,
        unread_count: 0,
      };
      setConversations((previous) => sortConversations([
        ...previous.filter((item) =>
          conversationKey(item) !== groupKey(detail.group.id)),
        conversation,
      ]));
      setGroupDetail(detail);
      setCreateGroupOpen(false);
      navigate(`/chat/group/${detail.group.id}`);
      void loadConversations(false);
    } catch (error) {
      if (!handleSessionError(error)) {
        setCreateGroupError(describeError(error));
      }
    } finally {
      setCreateGroupBusy(false);
    }
  }

  function openGroupInfo() {
    if (!activeGroupID) {
      return;
    }
    setGroupInfoOpen(true);
    setGroupActionError(null);
    setConfirmLeave(false);
    setInviteSelection([]);
    void loadGroupDetail(activeGroupID);
  }

  function toggleInviteSelection(username: string) {
    setInviteSelection((previous) =>
      previous.includes(username)
        ? previous.filter((item) => item !== username)
        : [...previous, username]);
    setGroupActionError(null);
  }

  async function renameActiveGroup() {
    if (!groupDetail?.is_owner || !activeGroupID) {
      return;
    }
    const name = groupNameDraft.trim();
    if (!name || [...name].length > MAX_GROUP_NAME_LENGTH) {
      setGroupActionError(`群名需要 1–${MAX_GROUP_NAME_LENGTH} 个字符。`);
      return;
    }
    setGroupActionBusy(true);
    setGroupActionError(null);
    try {
      const detail = await api.renameGroup(activeGroupID, name);
      setGroupDetail(detail);
      await loadConversations(false);
    } catch (error) {
      if (!handleSessionError(error)) {
        setGroupActionError(describeError(error));
      }
    } finally {
      setGroupActionBusy(false);
    }
  }

  async function inviteSelectedMembers() {
    if (!activeGroupID || inviteSelection.length === 0) {
      return;
    }
    setGroupActionBusy(true);
    setGroupActionError(null);
    try {
      const detail = await api.inviteGroupMembers(activeGroupID, inviteSelection);
      setGroupDetail(detail);
      setInviteSelection([]);
      await loadConversations(false);
    } catch (error) {
      if (!handleSessionError(error)) {
        setGroupActionError(describeError(error));
      }
    } finally {
      setGroupActionBusy(false);
    }
  }

  async function kickMember(username: string) {
    if (!activeGroupID) {
      return;
    }
    setGroupActionBusy(true);
    setGroupActionError(null);
    try {
      const detail = await api.kickGroupMember(activeGroupID, username);
      setGroupDetail(detail);
      await loadConversations(false);
    } catch (error) {
      if (!handleSessionError(error)) {
        setGroupActionError(describeError(error));
      }
    } finally {
      setGroupActionBusy(false);
    }
  }

  async function leaveActiveGroup() {
    if (!activeGroupID) {
      return;
    }
    setGroupActionBusy(true);
    setGroupActionError(null);
    const groupID = activeGroupID;
    const wasOwner = Boolean(groupDetail?.is_owner);
    try {
      const result = await api.leaveGroup(groupID);
      setGroupInfoOpen(false);
      setConfirmLeave(false);
      setConversations((previous) => previous.filter((conversation) =>
        !(conversation.type === "group" && conversation.group.id === groupID)));
      if (result.dissolved || wasOwner) {
        setChatNotice("群聊已解散。");
      } else {
        setChatNotice("已退出群聊。");
      }
      navigate("/");
    } catch (error) {
      if (!handleSessionError(error)) {
        setGroupActionError(describeError(error));
      }
    } finally {
      setGroupActionBusy(false);
    }
  }

  if (!user) {
    return null;
  }

  const draftLength = [...draft].length;
  const canSend = Boolean(activeDirect || activeGroup)
    && !isReadOnly
    && !isReplaced
    && !offline
    && !activeMissing
    && !messagesLoading;
  const reconnectExhausted = reconnectAttempt >= MAX_RECONNECT_ATTEMPTS;
  const reconnectText = reconnectAttempt > 0 && !reconnectExhausted
    ? `实时连接已断开，正在自动重连（第 ${reconnectAttempt} 次）…`
    : reconnectExhausted
      ? "实时连接已断开，自动重连未成功。"
      : "实时连接已断开。";
  const showReconnectBanner = !offline
    && (connectionStatus === "closed"
      || (connectionStatus === "connecting" && reconnectAttempt > 0));
  const workspaceClass = activeTarget
    ? "chat-workspace chat-workspace--active"
    : "chat-workspace";
  const inviteCandidates = groupDetail
    ? friends.filter((friend) => !groupDetail.members.some((member) =>
      member.user.username.toLocaleLowerCase()
        === friend.username.toLocaleLowerCase()))
    : [];
  const groupDisplayName = groupDetail?.group.name
    ?? activeGroup?.group.name
    ?? "群聊";
  const groupMemberCount = groupDetail?.group.member_count
    ?? activeGroup?.group.member_count
    ?? 0;

  return (
    <main className="chat-shell">
      <section className="chat-app" aria-label="聊天">
        <header className="chat-header">
          <Brand />
          <nav className="chat-nav" aria-label="主导航">
            <Link className="text-link" to="/friends">好友管理</Link>
            <Link className="text-link" to="/settings">账号设置</Link>
            <button
              className="text-link text-link--button"
              disabled={loggingOut}
              onClick={() => void handleLogout()}
              type="button"
            >
              {loggingOut ? "退出中…" : "退出"}
            </button>
          </nav>
        </header>

        {offline && (
          <div className="ws-banner ws-banner--offline" id="offline-banner" role="alert">
            <span>当前离线，只能查看已缓存的消息。重新联网后会自动恢复。</span>
          </div>
        )}

        {connectionStatus === "replaced" ? (
          <div className="ws-banner ws-banner--replaced" id="ws-replaced" role="alert">
            <span>聊天已在其他窗口打开。当前窗口不能发送新消息，历史仍可查看。</span>
            <button
              className="button button--compact button--secondary"
              onClick={() => navigate("/login?relogin=1")}
              type="button"
            >
              重新登录
            </button>
          </div>
        ) : showReconnectBanner ? (
          <div className="ws-banner" id="ws-status" role="status">
            <span>{reconnectText}</span>
            <button
              className="button button--compact button--secondary"
              onClick={manualReconnect}
              type="button"
            >
              {reconnectAttempt > 0 && !reconnectExhausted ? "立即重连" : "重新连接"}
            </button>
          </div>
        ) : null}

        {chatNotice && (
          <div className="ws-banner ws-banner--notice" id="chat-notice" role="status">
            <span>{chatNotice}</span>
            <button
              className="button button--compact button--secondary"
              onClick={() => setChatNotice(null)}
              type="button"
            >
              知道了
            </button>
          </div>
        )}

        <div className={workspaceClass}>
          <aside className="conversation-pane" aria-label="会话列表">
            <div className="conversation-pane-header">
              <div>
                <p className="eyebrow">消息</p>
                <h1>聊天</h1>
              </div>
              <div className="conversation-pane-actions">
                <span className="count-badge">{conversations.length}</span>
                <button
                  className="button button--compact button--secondary"
                  id="new-group-button"
                  onClick={openCreateGroup}
                  type="button"
                >
                  发起群聊
                </button>
              </div>
            </div>

            {conversationsError && (
              <div className="pane-error">
                <p className="form-error" role="alert">{conversationsError}</p>
                <button
                  className="button button--compact button--secondary"
                  onClick={() => void loadConversations(true)}
                  type="button"
                >
                  重试
                </button>
              </div>
            )}

            <div className="conversation-list" id="conversation-list">
              {conversationsLoading ? (
                <p className="empty-state">正在加载会话…</p>
              ) : conversations.length === 0 ? (
                <div className="conversation-empty">
                  <p>还没有好友或群聊。</p>
                  <Link className="text-link" to="/friends">去好友管理添加好友</Link>
                </div>
              ) : (
                conversations.map((conversation) => {
                  const key = conversationKey(conversation);
                  const isActive = key === activeKey;
                  const title = conversationTitle(conversation);
                  const link = conversation.type === "direct"
                    ? `/chat/${encodeURIComponent(conversation.user.username)}`
                    : `/chat/group/${conversation.group.id}`;
                  return (
                    <Link
                      aria-current={isActive ? "page" : undefined}
                      className={`conversation-item${isActive ? " conversation-item--active" : ""}${conversation.unread_count > 0 ? " conversation-item--unread" : ""}`}
                      data-group-id={conversation.type === "group" ? conversation.group.id : undefined}
                      data-username={conversation.type === "direct" ? conversation.user.username : undefined}
                      key={key}
                      to={link}
                    >
                      <div
                        className={`avatar avatar--small${conversation.type === "group" ? " avatar--group" : ""}`}
                        aria-hidden="true"
                      >
                        {initialOf(title)}
                      </div>
                      <div className="conversation-info">
                        <div className="conversation-line">
                          <span className="conversation-name">{title}</span>
                          <span className="conversation-meta">
                            {conversation.unread_count > 0 && (
                              <span className="unread-badge">
                                {conversation.unread_count > 99 ? "99+" : conversation.unread_count}
                              </span>
                            )}
                            {conversation.last_message && (
                              <time dateTime={conversation.last_message.created_at}>
                                {formatConversationTime(conversation.last_message.created_at)}
                              </time>
                            )}
                          </span>
                        </div>
                        <div className="conversation-line conversation-line--preview">
                          <span className="conversation-preview">
                            {previewOf(conversation, user.username)}
                          </span>
                          {conversation.type === "group" ? (
                            <span className="chat-badge">群聊</span>
                          ) : !conversation.is_friend ? (
                            <span className="chat-badge">非好友</span>
                          ) : null}
                        </div>
                      </div>
                    </Link>
                  );
                })
              )}
            </div>

            <div className="conversation-me">
              <div className="avatar avatar--tiny" aria-hidden="true">
                {initialOf(user.nickname)}
              </div>
              <div className="conversation-info">
                <p className="person-name">{user.nickname}</p>
                <p className="person-username">@{user.username}</p>
              </div>
            </div>
          </aside>

          <section className="message-pane" aria-label="当前对话">
            {!activeTarget ? (
              <div className="message-placeholder">
                <p className="eyebrow">{invalidGroupRoute ? "链接无效" : "选择会话"}</p>
                <h1>{invalidGroupRoute ? "找不到这个群聊" : "开始聊天"}</h1>
                <p>
                  {invalidGroupRoute
                    ? "群聊链接不正确，请从左侧会话列表选择。"
                    : "从左侧选择一个好友或群聊，发送第一条消息。"}
                </p>
              </div>
            ) : (
              <>
                <header className="message-header">
                  <div className="message-header-main">
                    <Link className="chat-back" id="chat-back" to="/" aria-label="返回会话列表">
                      ←
                    </Link>
                    <div>
                      {activeTarget.kind === "direct" ? (
                        <>
                          <h2>{activeUser?.nickname ?? activeUsername}</h2>
                          <p>@{activeUser?.username ?? activeUsername}</p>
                        </>
                      ) : (
                        <>
                          <h2>{groupDisplayName}</h2>
                          <p>{groupMemberCount} 位成员</p>
                        </>
                      )}
                    </div>
                  </div>
                  <div className="message-header-actions">
                    {activeTarget.kind === "group" && (
                      <button
                        className="button button--compact button--secondary"
                        id="group-info-button"
                        onClick={openGroupInfo}
                        type="button"
                      >
                        群信息
                      </button>
                    )}
                    <span
                      className={`connection-state connection-state--${connectionStatus}`}
                      title="当前窗口与服务器的实时连接状态"
                    >
                      {connectionStatus === "open"
                        ? "实时连接正常"
                        : connectionStatus === "connecting"
                          ? "连接中…"
                          : connectionStatus === "replaced"
                            ? "已被接管"
                            : "实时断开"}
                    </span>
                  </div>
                </header>

                {isReadOnly && (
                  <div className="chat-readonly" id="chat-readonly" role="status">
                    你们已不是好友，不能发送新消息。历史消息仍可查看。
                  </div>
                )}

                {activeTarget.kind === "group" && groupDetailError && (
                  <div className="pane-error pane-error--messages">
                    <p className="form-error" role="alert">{groupDetailError}</p>
                    <button
                      className="button button--compact button--secondary"
                      onClick={() => void loadGroupDetail(activeGroupID, true)}
                      type="button"
                    >
                      重新加载群信息
                    </button>
                  </div>
                )}

                {messagesError && (
                  <div className="pane-error pane-error--messages">
                    <p className="form-error" role="alert">{messagesError}</p>
                    <button
                      className="button button--compact button--secondary"
                      onClick={() => void loadInitialMessages(activeTarget)}
                      type="button"
                    >
                      重新加载
                    </button>
                  </div>
                )}

                <div
                  aria-live="polite"
                  className="message-list"
                  id="message-list"
                  onScroll={handleScroll}
                  ref={messageListRef}
                  role="log"
                >
                  {messagesLoading ? (
                    <p className="empty-state">正在加载消息…</p>
                  ) : !messagesBelongToActive ? (
                    <p className="empty-state">正在切换会话…</p>
                  ) : activeMissing ? (
                    <div className="message-placeholder message-placeholder--inline">
                      <p>
                        {activeTarget.kind === "group"
                          ? "找不到这个群聊。可能已解散，或你已不在群中。"
                          : "找不到这个会话。可能已不是好友，且没有历史消息。"}
                      </p>
                      <Link className="text-link" to="/">返回会话列表</Link>
                    </div>
                  ) : messages.length === 0 ? (
                    <div className="message-placeholder message-placeholder--inline">
                      <p>
                        {activeTarget.kind === "group"
                          ? "群里还没有消息，发一条打个招呼吧。"
                          : "还没有消息，发一条打个招呼吧。"}
                      </p>
                    </div>
                  ) : (
                    <>
                      {loadingOlder && <p className="message-loading">正在加载更早消息…</p>}
                      {!hasMore && <p className="message-start">没有更早的消息了</p>}
                      {messages.map((message, index) => {
                        const previous = messages[index - 1];
                        const showDay = !previous
                          || !isSameDay(previous.created_at, message.created_at);
                        const mine = sameUsername(message.sender.username, user.username);
                        const groupMessage = isGroupMessage(message);
                        return (
                          <Fragment key={message.client_message_id || message.id}>
                            {showDay && (
                              <div className="message-day">{formatDay(message.created_at)}</div>
                            )}
                            <div
                              className={`message-row${mine ? " message-row--mine" : ""}`}
                              data-client-message-id={message.client_message_id}
                              data-message-id={message.id || undefined}
                              data-status={message.status}
                            >
                              <div className="avatar avatar--tiny" aria-hidden="true">
                                {initialOf(message.sender.nickname)}
                              </div>
                              <div className="message-stack">
                                {groupMessage && !mine && (
                                  <span className="message-sender">{message.sender.nickname}</span>
                                )}
                                <div className={`message-bubble${mine ? " message-bubble--mine" : ""}${message.status === "failed" ? " message-bubble--failed" : ""}`}>
                                  {message.body}
                                </div>
                                <div className="message-meta">
                                  <time dateTime={message.created_at}>
                                    {formatMessageTime(message.created_at)}
                                  </time>
                                  {mine && message.status === "sending" && <span>发送中…</span>}
                                  {mine && message.status === "failed" && (
                                    <button
                                      className="message-retry"
                                      onClick={() => void retryMessage(message)}
                                      type="button"
                                    >
                                      发送失败，重试
                                    </button>
                                  )}
                                  {mine
                                    && !groupMessage
                                    && message.status === "sent"
                                    && message.id > 0
                                    && message.id === lastOwnSentMessageID && (
                                      <span className={`read-state${peerLastReadMessageID >= message.id ? " read-state--read" : ""}`}>
                                        {peerLastReadMessageID >= message.id ? "已读" : "未读"}
                                      </span>
                                    )}
                                  {mine
                                    && groupMessage
                                    && message.status === "sent"
                                    && message.id > 0
                                    && message.id === lastOwnSentMessageID
                                    && groupReceipt?.groupID === message.group_id
                                    && groupReceipt.messageID === message.id
                                    && groupReceipt.stats.member_count > 0 && (
                                      <span
                                        className={`read-state${groupReceipt.stats.all_read ? " read-state--read" : ""}`}
                                        data-member-count={groupReceipt.stats.member_count}
                                        data-read-count={groupReceipt.stats.read_count}
                                      >
                                        {groupReceiptLabel(groupReceipt.stats)}
                                      </span>
                                    )}
                                </div>
                              </div>
                            </div>
                          </Fragment>
                        );
                      })}
                    </>
                  )}
                </div>

                {sendError && <p className="form-error composer-error" role="alert">{sendError}</p>}
                {draftError && <p className="form-error composer-error" role="alert">{draftError}</p>}

                <form
                  className="composer"
                  id="message-form"
                  onSubmit={(event: FormEvent<HTMLFormElement>) => {
                    event.preventDefault();
                    void submitMessage();
                  }}
                >
                  <textarea
                    aria-label="消息内容"
                    disabled={!activeTarget || isReadOnly || offline || activeMissing || isReplaced}
                    id="message-input"
                    onChange={(event) => handleComposerChange(event.target.value)}
                    onKeyDown={handleComposerKeyDown}
                    placeholder={offline
                      ? "当前离线，无法发送消息"
                      : isReplaced
                        ? "聊天已在其他窗口打开"
                        : isReadOnly
                          ? "已不是好友，无法发送消息"
                          : activeTarget.kind === "group"
                            ? "输入群消息，Enter 发送，Shift+Enter 换行"
                            : "输入消息，Enter 发送，Shift+Enter 换行"}
                    ref={composerRef}
                    rows={1}
                    value={draft}
                  />
                  <button
                    className="button button--compact composer-send"
                    disabled={!canSend || draft.trim() === ""}
                    id="send-message"
                    type="submit"
                  >
                    发送
                  </button>
                  {draftLength > 0 && (
                    <span className={`composer-count${draftLength > MAX_MESSAGE_LENGTH ? " composer-count--over" : ""}`}>
                      {draftLength}/{MAX_MESSAGE_LENGTH}
                    </span>
                  )}
                </form>
              </>
            )}
          </section>
        </div>
      </section>

      {createGroupOpen && (
        <div
          className="modal-backdrop"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget && !createGroupBusy) {
              setCreateGroupOpen(false);
            }
          }}
        >
          <section
            aria-labelledby="create-group-title"
            aria-modal="true"
            className="modal"
            id="create-group-dialog"
            role="dialog"
          >
            <header className="modal-header">
              <div>
                <p className="eyebrow">新会话</p>
                <h2 id="create-group-title">发起群聊</h2>
              </div>
              <button
                className="text-link text-link--button"
                disabled={createGroupBusy}
                onClick={() => setCreateGroupOpen(false)}
                type="button"
              >
                关闭
              </button>
            </header>

            <label className="field" htmlFor="group-name-input">
              <span>群名</span>
              <input
                disabled={createGroupBusy}
                id="group-name-input"
                maxLength={MAX_GROUP_NAME_LENGTH}
                onChange={(event) => {
                  setCreateGroupName(event.target.value);
                  setCreateGroupError(null);
                }}
                placeholder="例如：周末出游"
                value={createGroupName}
              />
            </label>

            <div className="modal-section">
              <h3>选择好友</h3>
              <p className="modal-hint">只能邀请自己的好友，至少选择 1 位。</p>
              {friendsError && (
                <div className="pane-error">
                  <p className="form-error" role="alert">{friendsError}</p>
                  <button
                    className="button button--compact button--secondary"
                    onClick={() => void loadFriends()}
                    type="button"
                  >
                    重试
                  </button>
                </div>
              )}
              {friendsLoading ? (
                <p className="empty-state">正在加载好友…</p>
              ) : friends.length === 0 ? (
                <p className="modal-hint">
                  还没有好友，先<Link className="text-link" to="/friends">去好友管理添加</Link>。
                </p>
              ) : (
                <div className="friend-choice-list">
                  {friends.map((friend) => (
                    <label
                      className="friend-choice"
                      data-username={friend.username}
                      key={friend.username}
                    >
                      <input
                        checked={createGroupSelection.includes(friend.username)}
                        disabled={createGroupBusy}
                        onChange={() => toggleCreateGroupSelection(friend.username)}
                        type="checkbox"
                      />
                      <span>{friend.nickname}</span>
                      <span className="friend-choice-username">@{friend.username}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>

            {createGroupError && (
              <p className="form-error" role="alert">{createGroupError}</p>
            )}
            <div className="modal-actions">
              <button
                className="button button--compact"
                disabled={createGroupBusy}
                id="create-group-submit"
                onClick={() => void submitCreateGroup()}
                type="button"
              >
                {createGroupBusy ? "创建中…" : "创建群聊"}
              </button>
            </div>
          </section>
        </div>
      )}

      {groupInfoOpen && activeGroup && (
        <div
          className="modal-backdrop"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget && !groupActionBusy) {
              setGroupInfoOpen(false);
            }
          }}
        >
          <section
            aria-labelledby="group-info-title"
            aria-modal="true"
            className="modal modal--wide"
            id="group-info-panel"
            role="dialog"
          >
            <header className="modal-header">
              <div>
                <p className="eyebrow">群信息</p>
                <h2 id="group-info-title">{groupDisplayName}</h2>
              </div>
              <button
                className="text-link text-link--button"
                disabled={groupActionBusy}
                onClick={() => setGroupInfoOpen(false)}
                type="button"
              >
                关闭
              </button>
            </header>

            {groupDetailLoading && !groupDetail ? (
              <p className="empty-state">正在加载群信息…</p>
            ) : groupDetailError && !groupDetail ? (
              <div className="pane-error">
                <p className="form-error" role="alert">{groupDetailError}</p>
                <button
                  className="button button--compact button--secondary"
                  onClick={() => void loadGroupDetail(activeGroupID, true)}
                  type="button"
                >
                  重试
                </button>
              </div>
            ) : groupDetail ? (
              <>
                <div className="group-field">
                  <label htmlFor="group-info-name-input">群名</label>
                  <div className="group-field-row">
                    <input
                      disabled={!groupDetail.is_owner || groupActionBusy}
                      id="group-info-name-input"
                      maxLength={MAX_GROUP_NAME_LENGTH}
                      onChange={(event) => {
                        setGroupNameDraft(event.target.value);
                        setGroupActionError(null);
                      }}
                      value={groupNameDraft}
                    />
                    {groupDetail.is_owner && (
                      <button
                        className="button button--compact button--secondary"
                        disabled={groupActionBusy}
                        id="group-info-rename"
                        onClick={() => void renameActiveGroup()}
                        type="button"
                      >
                        保存
                      </button>
                    )}
                  </div>
                </div>

                <p className="group-meta">
                  {groupDetail.group.member_count} 位成员 · 群主 {groupDetail.group.owner.nickname}
                </p>

                <div className="modal-section">
                  <h3>成员</h3>
                  <ul className="group-member-list">
                    {groupDetail.members.map((member) => (
                      <li data-username={member.user.username} key={member.user.username}>
                        <span>
                          {member.user.nickname}
                          {member.is_owner ? "（群主）" : ""}
                        </span>
                        {groupDetail.is_owner && !member.is_owner && (
                          <button
                            className="text-link text-link--button text-link--danger"
                            disabled={groupActionBusy}
                            id={`group-info-kick-${member.user.username}`}
                            onClick={() => void kickMember(member.user.username)}
                            type="button"
                          >
                            移出
                          </button>
                        )}
                      </li>
                    ))}
                  </ul>
                </div>

                <div className="modal-section">
                  <h3>邀请好友</h3>
                  {friendsError && (
                    <p className="form-error" role="alert">{friendsError}</p>
                  )}
                  {friendsLoading ? (
                    <p className="empty-state">正在加载好友…</p>
                  ) : inviteCandidates.length === 0 ? (
                    <p className="modal-hint">没有可邀请的好友。</p>
                  ) : (
                    <>
                      <div className="friend-choice-list">
                        {inviteCandidates.map((friend) => (
                          <label
                            className="friend-choice"
                            data-username={friend.username}
                            key={friend.username}
                          >
                            <input
                              checked={inviteSelection.includes(friend.username)}
                              disabled={groupActionBusy}
                              onChange={() => toggleInviteSelection(friend.username)}
                              type="checkbox"
                            />
                            <span>{friend.nickname}</span>
                            <span className="friend-choice-username">@{friend.username}</span>
                          </label>
                        ))}
                      </div>
                      <button
                        className="button button--compact"
                        disabled={groupActionBusy || inviteSelection.length === 0}
                        id="group-info-invite"
                        onClick={() => void inviteSelectedMembers()}
                        type="button"
                      >
                        邀请
                      </button>
                    </>
                  )}
                </div>

                {groupActionError && (
                  <p className="form-error" role="alert">{groupActionError}</p>
                )}

                <div className="modal-actions modal-actions--split">
                  {!confirmLeave ? (
                    <button
                      className="button button--compact button--danger"
                      disabled={groupActionBusy}
                      id="group-info-leave"
                      onClick={() => setConfirmLeave(true)}
                      type="button"
                    >
                      {groupDetail.is_owner ? "解散群聊" : "退出群聊"}
                    </button>
                  ) : (
                    <div className="group-confirm">
                      <span>
                        {groupDetail.is_owner
                          ? "解散后所有成员都将无法再查看该群。"
                          : "退出后将无法再查看群历史。"}
                      </span>
                      <div>
                        <button
                          className="button button--compact button--danger"
                          disabled={groupActionBusy}
                          id="group-info-confirm-leave"
                          onClick={() => void leaveActiveGroup()}
                          type="button"
                        >
                          确认
                        </button>
                        <button
                          className="button button--compact button--secondary"
                          disabled={groupActionBusy}
                          onClick={() => setConfirmLeave(false)}
                          type="button"
                        >
                          取消
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              </>
            ) : null}
          </section>
        </div>
      )}
    </main>
  );
}
