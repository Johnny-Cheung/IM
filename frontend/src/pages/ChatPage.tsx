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
import { Link, useNavigate, useParams } from "react-router-dom";
import {
  api,
  ApiError,
  describeError,
  isSessionError,
  websocketURL,
  type ChatMessage,
  type Conversation,
} from "../api";
import { useAuth } from "../auth";
import { Brand } from "../components/Brand";

const PAGE_SIZE = 30;
const MAX_MESSAGE_LENGTH = 2000;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_BASE_DELAY = 1000;
const RECONNECT_MAX_DELAY = 10000;
const READ_DEBOUNCE_MS = 300;
const WS_CLOSE_REPLACED = 4001;
const WS_CLOSE_SESSION = 4002;

type ConnectionStatus = "connecting" | "open" | "closed" | "replaced";
type MessageStatus = "sent" | "sending" | "failed";
type DisplayMessage = ChatMessage & { status: MessageStatus };

type ReadEvent = {
  reader: string;
  peer: string;
  lastReadMessageID: number;
};

type ScrollPosition = {
  height: number;
  top: number;
};

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

function toDisplay(message: ChatMessage): DisplayMessage {
  return { ...message, status: "sent" };
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
      return right.last_message.id - left.last_message.id;
    }
    if (left.last_message) {
      return -1;
    }
    if (right.last_message) {
      return 1;
    }
    const leftName = left.user.nickname.toLocaleLowerCase();
    const rightName = right.user.nickname.toLocaleLowerCase();
    if (leftName !== rightName) {
      return leftName < rightName ? -1 : 1;
    }
    return left.user.username.localeCompare(right.user.username);
  });
}

function previewOf(conversation: Conversation, currentUsername: string): string {
  const message = conversation.last_message;
  if (!message) {
    return "还没有消息";
  }
  const prefix = message.sender.username === currentUsername ? "我：" : "";
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

export function ChatPage() {
  const { user, logout, invalidate } = useAuth();
  const navigate = useNavigate();
  const { username: routeUsername = "" } = useParams<{ username: string }>();
  const activeUsername = routeUsername.trim();
  const activeKey = activeUsername.toLocaleLowerCase();

  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [conversationsLoading, setConversationsLoading] = useState(true);
  const [conversationsError, setConversationsError] = useState<string | null>(null);
  const [messages, setMessages] = useState<DisplayMessage[]>([]);
  const [messagesUsername, setMessagesUsername] = useState("");
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

  const activeUsernameRef = useRef(activeUsername);
  const userRef = useRef(user);
  const conversationUsernamesRef = useRef<Set<string>>(new Set());
  const messageListRef = useRef<HTMLDivElement>(null);
  const composerRef = useRef<HTMLTextAreaElement>(null);
  const websocketRef = useRef<WebSocket | null>(null);
  const loggingOutRef = useRef(false);
  const messagesRef = useRef<DisplayMessage[]>([]);
  const pageVisibleRef = useRef(pageVisible);
  const replacedRef = useRef(false);
  const mountedRef = useRef(true);
  const reconnectTimerRef = useRef<number | null>(null);
  const reconnectAttemptRef = useRef(0);
  const connectRef = useRef<() => void>(() => {});
  const syncingRef = useRef(false);
  const readTimerRef = useRef<number | null>(null);
  const nearBottomRef = useRef(true);
  const scrollToBottomNextRef = useRef(false);
  const pendingOlderScrollRef = useRef<ScrollPosition | null>(null);
  const messagesRequestRef = useRef(0);
  const conversationVersionRef = useRef(0);

  useEffect(() => {
    activeUsernameRef.current = activeUsername;
  }, [activeUsername]);

  useEffect(() => {
    userRef.current = user;
  }, [user]);

  useEffect(() => {
    conversationUsernamesRef.current = new Set(
      conversations.map((conversation) => conversation.user.username),
    );
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

  const loadConversations = useCallback(async (initial = false) => {
    if (initial) {
      setConversationsLoading(true);
    }
    try {
      const response = await api.listConversations();
      setConversations(sortConversations(response.conversations));
      setConversationsError(null);
    } catch (error) {
      if (!handleSessionError(error)) {
        setConversationsError(describeError(error));
      }
    } finally {
      if (initial) {
        setConversationsLoading(false);
      }
    }
  }, [handleSessionError]);

  const loadInitialMessages = useCallback(async (targetUsername: string) => {
    const requestID = ++messagesRequestRef.current;
    setMessagesUsername(targetUsername);
    setMessagesLoading(true);
    setMessagesError(null);
    setMessages([]);
    setHasMore(false);
    setLoadingOlder(false);
    setDraftError(null);
    setSendError(null);
    setNotFriendLocally(false);
    nearBottomRef.current = true;
    pendingOlderScrollRef.current = null;

    try {
      const page = await api.listMessages(targetUsername, undefined, PAGE_SIZE);
      if (requestID !== messagesRequestRef.current) {
        return;
      }
      setMessages(page.messages.map(toDisplay));
      setHasMore(page.has_more);
      scrollToBottomNextRef.current = true;
    } catch (error) {
      if (requestID !== messagesRequestRef.current) {
        return;
      }
      if (!handleSessionError(error)) {
        setMessagesError(describeError(error));
      }
    } finally {
      if (requestID === messagesRequestRef.current) {
        setMessagesLoading(false);
      }
    }
  }, [handleSessionError]);

  useEffect(() => {
    void loadConversations(true);
  }, [loadConversations]);

  useEffect(() => {
    conversationVersionRef.current += 1;
    setDraft("");
    setDraftError(null);
    setSendError(null);
    if (!activeUsername) {
      messagesRequestRef.current += 1;
      setMessages([]);
      setMessagesUsername("");
      setMessagesError(null);
      setMessagesLoading(false);
      setHasMore(false);
      setLoadingOlder(false);
      setNotFriendLocally(false);
      return;
    }
    void loadInitialMessages(activeUsername);
  }, [activeUsername, loadInitialMessages]);

  const activeConversation = useMemo(
    () => conversations.find(
      (conversation) => conversation.user.username.toLocaleLowerCase() === activeKey,
    ),
    [conversations, activeKey],
  );
  const activeMissing = Boolean(activeUsername)
    && !conversationsLoading
    && !activeConversation;
  const isReadOnly = Boolean(activeConversation && !activeConversation.is_friend)
    || notFriendLocally;
  const isReplaced = connectionStatus === "replaced";
  const activeUser = activeConversation?.user;
  const messagesBelongToActive = activeKey !== ""
    && messagesUsername.toLocaleLowerCase() === activeKey;
  const peerLastReadMessageID = activeConversation?.peer_last_read_message_id ?? 0;
  const lastOwnSentMessageID = useMemo(() => {
    if (!user || !messagesBelongToActive) {
      return 0;
    }
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      const message = messages[index];
      if (message.sender.username === user.username && message.status === "sent") {
        return message.id;
      }
    }
    return 0;
  }, [messages, messagesBelongToActive, user]);

  const markRead = useCallback(async (targetUsername: string) => {
    try {
      await api.markConversationRead(targetUsername);
      setConversations((previous) => previous.map((conversation) =>
        conversation.user.username.toLocaleLowerCase() === targetUsername.toLocaleLowerCase()
          ? { ...conversation, unread_count: 0 }
          : conversation));
    } catch (error) {
      handleSessionError(error);
    }
  }, [handleSessionError]);

  useEffect(() => {
    if (!activeUsername || !messagesBelongToActive) {
      return;
    }
    if (messagesLoading || messages.length === 0) {
      return;
    }
    if (!pageVisible || connectionStatus === "replaced") {
      return;
    }
    if (readTimerRef.current !== null) {
      window.clearTimeout(readTimerRef.current);
    }
    readTimerRef.current = window.setTimeout(() => {
      readTimerRef.current = null;
      void markRead(activeUsername);
    }, READ_DEBOUNCE_MS);
    return () => {
      if (readTimerRef.current !== null) {
        window.clearTimeout(readTimerRef.current);
        readTimerRef.current = null;
      }
    };
  }, [
    activeUsername,
    connectionStatus,
    markRead,
    messages,
    messagesBelongToActive,
    messagesLoading,
    pageVisible,
  ]);

  const loadOlderMessages = useCallback(async () => {
    if (!activeUsername || !hasMore || loadingOlder || messagesLoading) {
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
      const page = await api.listMessages(activeUsername, oldest.id, PAGE_SIZE);
      if (version !== conversationVersionRef.current) {
        return;
      }
      pendingOlderScrollRef.current = { height: previousHeight, top: previousTop };
      setMessages((previous) => mergeMessages(previous, page.messages.map(toDisplay)));
      setHasMore(page.has_more);
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
    activeUsername,
    handleSessionError,
    hasMore,
    loadingOlder,
    messages,
    messagesLoading,
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
    const sentByCurrent = message.sender.username === currentUser.username;
    const receivedByCurrent = message.recipient.username === currentUser.username;
    if (!sentByCurrent && !receivedByCurrent) {
      return;
    }
    const otherUser = sentByCurrent ? message.recipient : message.sender;
    const incoming = receivedByCurrent;
    const readingNow = activeUsernameRef.current.toLocaleLowerCase()
      === otherUser.username.toLocaleLowerCase()
      && pageVisibleRef.current
      && !replacedRef.current;

    if (conversationUsernamesRef.current.has(otherUser.username)) {
      setConversations((previous) => sortConversations(previous.map((conversation) =>
        conversation.user.username === otherUser.username
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

    const active = activeUsernameRef.current.toLocaleLowerCase();
    const belongsToActive = active !== ""
      && (message.sender.username.toLocaleLowerCase() === active
        || message.recipient.username.toLocaleLowerCase() === active);
    if (belongsToActive) {
      const wasNearBottom = nearBottomRef.current;
      setMessages((previous) => mergeMessages(previous, [toDisplay(message)]));
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
    if (event.reader === currentUser.username) {
      setConversations((previous) => previous.map((conversation) =>
        conversation.user.username === event.peer
          ? { ...conversation, unread_count: 0 }
          : conversation));
      return;
    }
    if (event.peer === currentUser.username) {
      setConversations((previous) => previous.map((conversation) =>
        conversation.user.username === event.reader
          ? {
            ...conversation,
            peer_last_read_message_id: Math.max(
              conversation.peer_last_read_message_id,
              event.lastReadMessageID,
            ),
          }
          : conversation));
    }
  }, []);

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

  const syncActiveMessages = useCallback(async (targetUsername: string) => {
    if (syncingRef.current) {
      return;
    }
    const newestID = messagesRef.current.reduce(
      (newest, message) => message.id > 0 ? Math.max(newest, message.id) : newest,
      0,
    );
    if (newestID === 0) {
      try {
        const page = await api.listMessages(targetUsername, undefined, PAGE_SIZE);
        if (activeUsernameRef.current.toLocaleLowerCase() !== targetUsername.toLocaleLowerCase()) {
          return;
        }
        setMessages((previous) => mergeMessages(previous, page.messages.map(toDisplay)));
        setHasMore(page.has_more);
        setMessagesError(null);
        if (nearBottomRef.current) {
          scrollToBottomNextRef.current = true;
        }
      } catch (error) {
        if (!handleSessionError(error)) {
          setMessagesError(describeError(error));
        }
      }
      return;
    }

    syncingRef.current = true;
    try {
      let afterID = newestID;
      let hasMore = true;
      for (let pageIndex = 0; pageIndex < 10 && hasMore; pageIndex += 1) {
        const page = await api.listMessagesAfter(targetUsername, afterID, 50);
        if (activeUsernameRef.current.toLocaleLowerCase() !== targetUsername.toLocaleLowerCase()) {
          return;
        }
        if (page.messages.length > 0) {
          setMessages((previous) => mergeMessages(previous, page.messages.map(toDisplay)));
          afterID = page.messages[page.messages.length - 1].id;
        }
        hasMore = page.has_more;
      }
      if (
        hasMore
        && activeUsernameRef.current.toLocaleLowerCase() === targetUsername.toLocaleLowerCase()
      ) {
        await loadInitialMessages(targetUsername);
        return;
      }
      if (nearBottomRef.current) {
        scrollToBottomNextRef.current = true;
      }
    } catch (error) {
      if (!handleSessionError(error)) {
        await loadInitialMessages(targetUsername).catch(() => undefined);
      }
    } finally {
      syncingRef.current = false;
    }
  }, [handleSessionError, loadInitialMessages]);

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
      void loadConversations(false);
      const active = activeUsernameRef.current;
      if (active) {
        void syncActiveMessages(active);
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
        } else if (data?.type === "messages.read") {
          const readEvent = parseReadEvent(data);
          if (readEvent) {
            handleReadEvent(readEvent);
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
    handleIncomingMessage,
    handleReadEvent,
    invalidate,
    loadConversations,
    navigate,
    scheduleReconnect,
    syncActiveMessages,
  ]);

  useEffect(() => {
    connectRef.current = () => connectWebSocket(false);
  }, [connectWebSocket]);

  useEffect(() => {
    mountedRef.current = true;
    connectWebSocket(true);
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
  }, [connectWebSocket]);

  useEffect(() => {
    const handleVisibility = () => {
      const visible = document.visibilityState === "visible";
      setPageVisible(visible);
      if (!visible) {
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
      if (document.visibilityState === "visible") {
        void loadConversations(false);
      }
    };
    const handleOnline = () => {
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
  }, [loadConversations]);

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

  async function deliverMessage(message: DisplayMessage, targetUsername: string) {
    try {
      const response = await api.sendMessage(
        targetUsername,
        message.body,
        message.client_message_id,
      );
      const sent = toDisplay(response.message);
      if (activeUsernameRef.current.toLocaleLowerCase() === targetUsername.toLocaleLowerCase()) {
        setMessages((previous) => mergeMessages(previous, [sent]));
      }
      if (conversationUsernamesRef.current.has(targetUsername)) {
        setConversations((previous) => sortConversations(previous.map((conversation) =>
          conversation.user.username.toLocaleLowerCase() === targetUsername.toLocaleLowerCase()
            ? { ...conversation, last_message: sent }
            : conversation)));
      } else {
        void loadConversations(false);
      }
      setSendError(null);
      composerRef.current?.focus();
    } catch (error) {
      if (handleSessionError(error)) {
        return;
      }
      setMessages((previous) => previous.map((item) =>
        item.client_message_id === message.client_message_id
          ? { ...item, status: "failed" }
          : item));
      if (error instanceof ApiError && error.code === "not_friends") {
        setNotFriendLocally(true);
        setConversations((previous) => previous.map((conversation) =>
          conversation.user.username.toLocaleLowerCase() === targetUsername.toLocaleLowerCase()
            ? { ...conversation, is_friend: false }
            : conversation));
      }
      setSendError(describeError(error));
    }
  }

  async function submitMessage() {
    if (!user || !activeConversation || isReadOnly || isReplaced) {
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

    const message: DisplayMessage = {
      id: 0,
      sender: { username: user.username, nickname: user.nickname },
      recipient: activeConversation.user,
      body,
      client_message_id: newClientMessageID(),
      created_at: new Date().toISOString(),
      status: "sending",
    };
    setMessages((previous) => mergeMessages(previous, [message]));
    setDraft("");
    setDraftError(null);
    setSendError(null);
    scrollToBottomNextRef.current = true;
    await deliverMessage(message, activeConversation.user.username);
  }

  async function retryMessage(message: DisplayMessage) {
    if (isReadOnly || isReplaced) {
      return;
    }
    setMessages((previous) => previous.map((item) =>
      item.client_message_id === message.client_message_id
        ? { ...item, status: "sending" }
        : item));
    setSendError(null);
    await deliverMessage({ ...message, status: "sending" }, message.recipient.username);
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

  if (!user) {
    return null;
  }

  const draftLength = [...draft].length;
  const canSend = Boolean(activeConversation)
    && !isReadOnly
    && !isReplaced
    && !activeMissing
    && !messagesLoading;
  const reconnectExhausted = reconnectAttempt >= MAX_RECONNECT_ATTEMPTS;
  const reconnectText = reconnectAttempt > 0 && !reconnectExhausted
    ? `实时连接已断开，正在自动重连（第 ${reconnectAttempt} 次）…`
    : reconnectExhausted
      ? "实时连接已断开，自动重连未成功。"
      : "实时连接已断开。";
  const showReconnectBanner = connectionStatus === "closed"
    || (connectionStatus === "connecting" && reconnectAttempt > 0);
  const workspaceClass = activeKey
    ? "chat-workspace chat-workspace--active"
    : "chat-workspace";

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

        <div className={workspaceClass}>
          <aside className="conversation-pane" aria-label="会话列表">
            <div className="conversation-pane-header">
              <div>
                <p className="eyebrow">消息</p>
                <h1>聊天</h1>
              </div>
              <span className="count-badge">{conversations.length}</span>
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
                  <p>还没有好友。</p>
                  <Link className="text-link" to="/friends">去好友管理添加</Link>
                </div>
              ) : (
                conversations.map((conversation) => {
                  const isActive = conversation.user.username.toLocaleLowerCase() === activeKey;
                  return (
                    <Link
                      aria-current={isActive ? "page" : undefined}
                      className={`conversation-item${isActive ? " conversation-item--active" : ""}${conversation.unread_count > 0 ? " conversation-item--unread" : ""}`}
                      data-username={conversation.user.username}
                      key={conversation.user.username}
                      to={`/chat/${encodeURIComponent(conversation.user.username)}`}
                    >
                      <div className="avatar avatar--small" aria-hidden="true">
                        {initialOf(conversation.user.nickname)}
                      </div>
                      <div className="conversation-info">
                        <div className="conversation-line">
                          <span className="conversation-name">{conversation.user.nickname}</span>
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
                          {!conversation.is_friend && (
                            <span className="chat-badge">非好友</span>
                          )}
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
            {!activeUsername ? (
              <div className="message-placeholder">
                <p className="eyebrow">选择会话</p>
                <h1>开始聊天</h1>
                <p>从左侧选择一个好友，发送第一条消息。</p>
              </div>
            ) : (
              <>
                <header className="message-header">
                  <div className="message-header-main">
                    <Link className="chat-back" id="chat-back" to="/" aria-label="返回会话列表">
                      ←
                    </Link>
                    <div>
                      <h2>{activeUser?.nickname ?? activeUsername}</h2>
                      <p>@{activeUser?.username ?? activeUsername}</p>
                    </div>
                  </div>
                  <span className={`connection-state connection-state--${connectionStatus}`}>
                    {connectionStatus === "open"
                      ? "实时在线"
                      : connectionStatus === "connecting"
                        ? "连接中…"
                        : connectionStatus === "replaced"
                          ? "已被接管"
                          : "实时断开"}
                  </span>
                </header>

                {isReadOnly && (
                  <div className="chat-readonly" id="chat-readonly" role="status">
                    你们已不是好友，不能发送新消息。历史消息仍可查看。
                  </div>
                )}

                {messagesError && (
                  <div className="pane-error pane-error--messages">
                    <p className="form-error" role="alert">{messagesError}</p>
                    <button
                      className="button button--compact button--secondary"
                      onClick={() => void loadInitialMessages(activeUsername)}
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
                      <p>找不到这个会话。可能已不是好友，且没有历史消息。</p>
                      <Link className="text-link" to="/">返回会话列表</Link>
                    </div>
                  ) : messages.length === 0 ? (
                    <div className="message-placeholder message-placeholder--inline">
                      <p>还没有消息，发一条打个招呼吧。</p>
                    </div>
                  ) : (
                    <>
                      {loadingOlder && <p className="message-loading">正在加载更早消息…</p>}
                      {!hasMore && <p className="message-start">没有更早的消息了</p>}
                      {messages.map((message, index) => {
                        const previous = messages[index - 1];
                        const showDay = !previous
                          || !isSameDay(previous.created_at, message.created_at);
                        const mine = message.sender.username === user.username;
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
                                    && message.status === "sent"
                                    && message.id > 0
                                    && message.id === lastOwnSentMessageID && (
                                      <span className={`read-state${peerLastReadMessageID >= message.id ? " read-state--read" : ""}`}>
                                        {peerLastReadMessageID >= message.id ? "已读" : "未读"}
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
                    disabled={!activeConversation || isReadOnly || activeMissing || isReplaced}
                    id="message-input"
                    onChange={(event) => handleComposerChange(event.target.value)}
                    onKeyDown={handleComposerKeyDown}
                    placeholder={isReplaced
                      ? "聊天已在其他窗口打开"
                      : isReadOnly
                        ? "已不是好友，无法发送消息"
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
    </main>
  );
}
