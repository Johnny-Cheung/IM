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
  isAcceptedSeq,
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
  type User,
} from "../api";
import {
  deleteMessagesFrom,
  deleteOutbox,
  markOutboxFailed,
  readConversations,
  readFriends,
  readMessages,
  readOutbox,
  readSyncState,
  removeMessages,
  writeConversations,
  writeFriends,
  writeMessages,
  writeOutbox,
} from "../cache";
import type { CachedMessage, CachedOutbox } from "../cache";
import { BLUR_FRAME, focusFrame, parseFocusAck, type FocusAck, type FocusTarget } from "../focus";
import { groupReceiptLabel, lastOwnSentSeq as findLastOwnSentSeq } from "../groupReceipt";
import {
  Debouncer,
  READ_STATS_DEBOUNCE_MS,
  ReadStatsGate,
  parseGroupReadEvent,
  parseGroupReadStats,
  type GroupReadEvent,
} from "../readStats";
import {
  acceptedSeqMap,
  isReadByPeer,
  lostOutboxEntries,
  maxAcceptedSeq,
  missingSeqs,
  MissingSeqTracker,
  nextUnread,
  outboxDisplayStatus,
  planActiveSync,
  planGapPull,
  planIncoming,
  planListRollback,
  planRollback,
  shouldReplacePreview,
  sortConversationMessages,
} from "../seqSync";
import { Brand } from "../components/Brand";
import { useAuth } from "../auth";

const PAGE_SIZE = 30;
const MAX_GAP_SCAN = 2000;
const MAX_MESSAGE_LENGTH = 2000;
const MAX_GROUP_NAME_LENGTH = 30;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_BASE_DELAY = 1000;
const RECONNECT_MAX_DELAY = 10000;
const WS_CLOSE_REPLACED = 4001;
const WS_CLOSE_SESSION = 4002;

// Client ids whose send request is running in this document. A stored outbox
// row for one of them is still sending; any other stored "sending" row was
// orphaned by a reload and must surface as failed.
const inFlightSends = new Set<string>();

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
  lastReadSeq: number;
};

type GroupChangedEvent = {
  groupID: number;
  action: string;
};

type GroupReceipt = {
  groupID: number;
  seq: number;
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

function focusTargetKey(target: FocusTarget): string {
  return target.kind === "direct"
    ? directKey(target.username)
    : groupKey(target.groupID);
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
  return sortConversationMessages(messages);
}

function mergeMessages(
  existing: DisplayMessage[],
  incoming: DisplayMessage[],
): DisplayMessage[] {
  const merged = [...existing];
  for (const message of incoming) {
    const confirmed = isAcceptedSeq(message.seq);
    let index = confirmed
      ? merged.findIndex((item) =>
        isAcceptedSeq(item.seq) && item.seq === message.seq)
      : -1;
    if (index < 0 && message.client_message_id !== "") {
      // Any incoming message replaces its local pending copy by client id;
      // confirmed messages additionally replace by seq above.
      index = merged.findIndex((item) =>
        !isAcceptedSeq(item.seq)
        && item.client_message_id === message.client_message_id);
    }
    if (index >= 0) {
      const current = merged[index];
      if (
        confirmed
        && isAcceptedSeq(current.seq)
        && current.seq === message.seq
        && current.id !== message.id
      ) {
        // A reused seq makes every higher cached seq stale.
        for (let position = merged.length - 1; position >= 0; position -= 1) {
          const item = merged[position];
          if (isAcceptedSeq(item.seq) && item.seq > message.seq) {
            merged.splice(position, 1);
          }
        }
      }
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
    || !isAcceptedSeq(candidate.seq)
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
    || !isAcceptedSeq(candidate.seq)
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
    last_read_seq?: unknown;
  };
  if (
    event.type !== "messages.read"
    || typeof event.reader !== "string"
    || typeof event.peer !== "string"
    || typeof event.last_read_seq !== "number"
    || !Number.isSafeInteger(event.last_read_seq)
    || event.last_read_seq < 0
  ) {
    return null;
  }
  return {
    reader: event.reader,
    peer: event.peer,
    lastReadSeq: event.last_read_seq,
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

function acceptedPage<T extends { seq: number }>(messages: T[]): T[] {
  // Accepted messages always carry seq >= 1; a page entry without one is
  // unusable for ordering and must not reach the cache or the list.
  return messages.filter((message) => isAcceptedSeq(message.seq));
}

async function fetchInitialPage(
  target: ActiveTarget,
): Promise<{ messages: DisplayMessage[]; hasMore: boolean }> {
  if (target.kind === "direct") {
    const page = await api.listMessages(target.username, undefined, PAGE_SIZE);
    return {
      messages: acceptedPage(page.messages).map(toDirectDisplay),
      hasMore: page.has_more,
    };
  }
  const page = await api.listGroupMessages(target.groupID, undefined, PAGE_SIZE);
  return {
    messages: acceptedPage(page.messages).map(toGroupDisplay),
    hasMore: page.has_more,
  };
}

async function fetchOlderPage(
  target: ActiveTarget,
  beforeSeq: number,
): Promise<{ messages: DisplayMessage[]; hasMore: boolean }> {
  if (target.kind === "direct") {
    const page = await api.listMessages(target.username, beforeSeq, PAGE_SIZE);
    return {
      messages: acceptedPage(page.messages).map(toDirectDisplay),
      hasMore: page.has_more,
    };
  }
  const page = await api.listGroupMessages(target.groupID, beforeSeq, PAGE_SIZE);
  return {
    messages: acceptedPage(page.messages).map(toGroupDisplay),
    hasMore: page.has_more,
  };
}

async function fetchAfterPage(
  target: ActiveTarget,
  afterSeq: number,
): Promise<{ messages: DisplayMessage[]; hasMore: boolean }> {
  if (target.kind === "direct") {
    const page = await api.listMessagesAfter(target.username, afterSeq, 50);
    return {
      messages: acceptedPage(page.messages).map(toDirectDisplay),
      hasMore: page.has_more,
    };
  }
  const page = await api.listGroupMessagesAfter(target.groupID, afterSeq, 50);
  return {
    messages: acceptedPage(page.messages).map(toGroupDisplay),
    hasMore: page.has_more,
  };
}

type TailPull = {
  messages: DisplayMessage[];
  lastSeq: number;
  truncated: boolean;
};

async function collectAfterSeq(
  target: ActiveTarget,
  afterSeq: number,
): Promise<TailPull> {
  let after = afterSeq;
  const messages: DisplayMessage[] = [];
  for (let round = 0; round < 10; round += 1) {
    const page = await fetchAfterPage(target, after);
    if (page.messages.length === 0) {
      return { messages, lastSeq: after, truncated: false };
    }
    messages.push(...page.messages);
    const next = maxAcceptedSeq(page.messages);
    if (next <= after) {
      return { messages, lastSeq: after, truncated: false };
    }
    after = next;
    if (!page.hasMore) {
      return { messages, lastSeq: after, truncated: false };
    }
  }
  return { messages, lastSeq: after, truncated: true };
}

// Pulls the ranges the local cache is missing. Seqs the server skips after a
// pull are marked and never retried, so one lost message cannot loop forever.
async function pullGaps(
  target: ActiveTarget,
  fromSeq: number,
  toSeq: number,
  present: Set<number>,
  tracker: MissingSeqTracker,
): Promise<DisplayMessage[]> {
  const collected: DisplayMessage[] = [];
  for (let round = 0; round < 3; round += 1) {
    const plan = planGapPull(
      fromSeq,
      toSeq,
      present,
      tracker.pending(target.key),
    );
    if (plan === null) {
      break;
    }
    const pulled = await collectAfterSeq(target, plan.afterSeq);
    for (const message of pulled.messages) {
      present.add(message.seq);
      collected.push(message);
    }
    const reached = Math.min(pulled.lastSeq, plan.targetSeq);
    tracker.mark(
      target.key,
      missingSeqs(plan.afterSeq, reached, present),
    );
    if (pulled.lastSeq <= plan.afterSeq) {
      tracker.mark(
        target.key,
        missingSeqs(plan.afterSeq, plan.targetSeq, present),
      );
      break;
    }
    if (!pulled.truncated && pulled.lastSeq < plan.targetSeq) {
      tracker.mark(
        target.key,
        missingSeqs(pulled.lastSeq, plan.targetSeq, present),
      );
    }
  }
  return collected;
}

function outboxDisplay(
  entry: CachedOutbox,
  user: User,
): DisplayMessage | null {
  const base = {
    id: 0,
    seq: 0,
    sender: { username: user.username, nickname: user.nickname },
    body: entry.body,
    client_message_id: entry.client_message_id,
    created_at: entry.created_at,
    status: outboxDisplayStatus(
      entry.status,
      inFlightSends.has(entry.client_message_id),
    ),
  };
  if (entry.kind === "group") {
    if (entry.group_id === undefined) {
      return null;
    }
    return { ...base, group_id: entry.group_id };
  }
  if (!entry.recipient) {
    return null;
  }
  return { ...base, recipient: entry.recipient };
}

function outboxFailedMessages(
  entries: readonly CachedOutbox[],
  confirmed: Iterable<string>,
  user: User,
): DisplayMessage[] {
  return lostOutboxEntries(entries, new Set(confirmed))
    .map((entry) => outboxDisplay(entry, user))
    .filter((message): message is DisplayMessage => message !== null);
}

// A rollback writes archived failures after the outbox snapshot was read; add
// them so a page reload that replaces the list still shows them once.
function mergeOutboxEntries(
  current: readonly CachedOutbox[],
  archived: readonly CachedOutbox[],
): CachedOutbox[] {
  const byClientID = new Map(
    current.map((entry) => [entry.client_message_id, entry]),
  );
  for (const entry of archived) {
    byClientID.set(entry.client_message_id, entry);
  }
  return [...byClientID.values()];
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
  const conversationLastSeqsRef = useRef<Map<string, number>>(new Map());
  const conversationLastMessagesRef = useRef<Map<string, { id: number; seq: number }>>(
    new Map(),
  );
  const missingSeqTrackerRef = useRef(new MissingSeqTracker());
  const resyncTailRef = useRef<(target: ActiveTarget, keepSeq: number) => void>(
    () => {},
  );
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
  const groupReceiptTargetRef = useRef<{ groupID: number; seq: number } | null>(null);
  const readStatsGateRef = useRef(new ReadStatsGate());
  const readStatsDebouncerRef = useRef(new Debouncer(READ_STATS_DEBOUNCE_MS));
  const nearBottomRef = useRef(true);
  const scrollToBottomNextRef = useRef(false);
  const pendingOlderScrollRef = useRef<ScrollPosition | null>(null);
  const messagesRequestRef = useRef(0);
  const conversationVersionRef = useRef(0);
  const conversationRequestRef = useRef(0);
  const conversationsInFlightRef = useRef(0);
  const conversationsRefetchRef = useRef(false);
  const conversationsRefetchDirtyRef = useRef(false);
  const pendingFocusReadsRef = useRef<Map<string, FocusTarget>>(new Map());
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
    const lastSeqs = new Map<string, number>();
    const lastMessages = new Map<string, { id: number; seq: number }>();
    for (const conversation of conversations) {
      const key = conversationKey(conversation);
      lastSeqs.set(key, conversation.last_seq);
      if (
        conversation.last_message
        && isAcceptedSeq(conversation.last_message.seq)
      ) {
        lastMessages.set(key, {
          id: conversation.last_message.id,
          seq: conversation.last_message.seq,
        });
      }
    }
    conversationLastSeqsRef.current = lastSeqs;
    conversationLastMessagesRef.current = lastMessages;
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

  const clearLocalUnread = useCallback((target: FocusTarget, throughSeq: number) => {
    setConversations((previous) => previous.map((conversation) => {
      if (target.kind === "direct") {
        return conversation.type === "direct"
          && sameUsername(conversation.user.username, target.username)
          && conversation.last_seq <= throughSeq
          ? { ...conversation, unread_count: 0 }
          : conversation;
      }
      return conversation.type === "group"
        && conversation.group.id === target.groupID
        && conversation.last_seq <= throughSeq
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
    const visible = pageVisibleRef.current;
    if (visible) {
      // Replay conversations read while the socket was down; the server
      // advances each one, then the current focus stays last.
      const currentKey = target === null ? "" : focusTargetKey(target);
      for (const [key, pending] of pendingFocusReadsRef.current) {
        if (key !== currentKey) {
          socket.send(JSON.stringify(focusFrame(pending)));
        }
      }
      pendingFocusReadsRef.current.clear();
    }
    const frame = target !== null && visible ? focusFrame(target) : BLUR_FRAME;
    socket.send(JSON.stringify(frame));
  }, []);

  const fetchGroupReadStats = useCallback(async () => {
    const target = groupReceiptTargetRef.current;
    if (target === null || offlineRef.current) {
      return;
    }
    const token = readStatsGateRef.current.begin(
      target.groupID,
      target.seq,
    );
    try {
      const stats = parseGroupReadStats(
        await api.groupReadStats(target.groupID, target.seq),
      );
      if (
        stats === null
        || !readStatsGateRef.current.accepts(
          token,
          target.groupID,
          target.seq,
        )
        || stats.seq !== target.seq
      ) {
        return;
      }
      // The seq may have been reused by a different message after a rollback;
      // drop a response that no longer points at the message on screen.
      const local = messagesRef.current.find((message) =>
        isGroupMessage(message)
        && message.group_id === target.groupID
        && message.seq === target.seq);
      if (local === undefined || local.id !== stats.message_id) {
        return;
      }
      setGroupReceipt({
        groupID: target.groupID,
        seq: stats.seq,
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

  // A rollback archives the user's own lost messages into the outbox; keep them
  // visible as failed rows in the open conversation so they can be retried.
  const showArchivedFailures = useCallback((
    target: ActiveTarget,
    archived: readonly CachedOutbox[],
  ) => {
    const currentUser = userRef.current;
    if (
      currentUser === null
      || archived.length === 0
      || activeTargetRef.current?.key !== target.key
    ) {
      return;
    }
    const failed = archived
      .map((entry) => outboxDisplay(entry, currentUser))
      .filter((message): message is DisplayMessage => message !== null);
    if (failed.length > 0) {
      setMessages((previous) => mergeMessages(previous, failed));
    }
  }, []);

  const resyncTail = useCallback(async (target: ActiveTarget, keepSeq: number) => {
    if (!user) {
      return;
    }
    missingSeqTrackerRef.current.clear(target.key);
    try {
      const pulled = await collectAfterSeq(target, keepSeq);
      if (activeTargetRef.current?.key !== target.key) {
        return;
      }
      if (pulled.truncated) {
        // A long backlog is faster to reload from the newest page; the local
        // watermark is rebuilt by the write below.
        const page = await fetchInitialPage(target);
        if (activeTargetRef.current?.key !== target.key) {
          return;
        }
        const outbox = await readOutbox(user.username, target.key);
        const confirmed = new Set(
          page.messages.map((message) => message.client_message_id),
        );
        setMessages([
          ...page.messages,
          ...outboxFailedMessages(outbox, confirmed, user),
        ]);
        setHasMore(page.hasMore);
        await writeMessages(user.username, target.key, page.messages);
        scrollToBottomNextRef.current = true;
        return;
      }
      const present = new Set(pulled.messages.map((message) => message.seq));
      const gapMessages = await pullGaps(
        target,
        keepSeq,
        pulled.lastSeq,
        present,
        missingSeqTrackerRef.current,
      );
      const messages = [...pulled.messages, ...gapMessages];
      if (messages.length === 0) {
        return;
      }
      setMessages((previous) => mergeMessages(previous, messages));
      await writeMessages(user.username, target.key, messages);
      if (nearBottomRef.current) {
        scrollToBottomNextRef.current = true;
      }
    } catch (error) {
      handleSessionError(error);
    }
  }, [handleSessionError, user]);

  useEffect(() => {
    resyncTailRef.current = (target, keepSeq) => {
      void resyncTail(target, keepSeq);
    };
  }, [resyncTail]);

  const loadConversations = useCallback(async (initial = false) => {
    const requestID = ++conversationRequestRef.current;
    conversationsInFlightRef.current += 1;
    if (initial) {
      setConversationsLoading(true);
    }
    try {
      const response = await api.listConversations();
      if (requestID !== conversationRequestRef.current) {
        return;
      }
      const username = userRef.current?.username ?? "";
      const rolledBack: Array<{ key: string; keepSeq: number }> = [];
      if (username !== "") {
        const sync = await readSyncState(username);
        if (requestID !== conversationRequestRef.current) {
          return;
        }
        for (const conversation of response.conversations) {
          const key = conversationKey(conversation);
          // Compare the server list against the locally persisted watermark,
          // never against the server value under suspicion.
          const liveMax = activeTargetRef.current?.key === key
            ? maxAcceptedSeq(messagesRef.current.filter((message) =>
              conversation.type === "group"
                ? isGroupMessage(message)
                  && message.group_id === conversation.group.id
                : !isGroupMessage(message)
                  && (sameUsername(message.sender.username, conversation.user.username)
                    || sameUsername(message.recipient.username, conversation.user.username))))
            : 0;
          const localMax = Math.max(sync.localSeq[key] ?? 0, liveMax);
          const rollback = planRollback(conversation.last_seq, localMax);
          if (rollback !== null) {
            rolledBack.push({ key, keepSeq: rollback.fromSeq - 1 });
          }
        }
      }
      if (rolledBack.length > 0 && username !== "") {
        if (requestID !== conversationRequestRef.current) {
          return;
        }
        const archived = new Map<string, CachedOutbox[]>();
        await Promise.all(rolledBack.map(async (entry) => {
          archived.set(
            entry.key,
            await deleteMessagesFrom(username, entry.key, entry.keepSeq + 1),
          );
        }));
        // Cache cleanup can outlive this request; a newer list response that
        // already landed must not be overwritten by this stale snapshot.
        if (requestID !== conversationRequestRef.current) {
          return;
        }
        const active = activeTargetRef.current;
        const current = rolledBack.find((entry) => entry.key === active?.key);
        if (active && current) {
          setMessages((previous) => previous.filter((message) =>
            !isAcceptedSeq(message.seq) || message.seq <= current.keepSeq));
          showArchivedFailures(active, archived.get(current.key) ?? []);
          missingSeqTrackerRef.current.clear(current.key);
          resyncTailRef.current(active, current.keepSeq);
        }
      }
      const pendingReads = pendingFocusReadsRef.current;
      setConversations(sortConversations(response.conversations.map((conversation) =>
        pendingReads.has(conversationKey(conversation))
          ? { ...conversation, unread_count: 0 }
          : conversation)));
      setConversationsError(null);
    } catch (error) {
      if (!handleSessionError(error) && requestID === conversationRequestRef.current) {
        setConversationsError(describeError(error));
      }
    } finally {
      conversationsInFlightRef.current -= 1;
      if (requestID === conversationRequestRef.current) {
        setConversationsLoading(false);
      }
    }
  }, [handleSessionError, showArchivedFailures]);

  const handleFocusAck = useCallback((ack: FocusAck) => {
    if (!ack.ok) {
      return;
    }
    clearLocalUnread(ack.target, ack.lastReadSeq);
    pendingFocusReadsRef.current.delete(focusTargetKey(ack.target));
    if (conversationsInFlightRef.current === 0) {
      return;
    }
    // A list response snapshotted before this ack can still overwrite the
    // unread it just cleared; refetch until the last ack is reflected.
    if (conversationsRefetchRef.current) {
      conversationsRefetchDirtyRef.current = true;
      return;
    }
    const refresh = () => {
      conversationsRefetchRef.current = true;
      void loadConversations(false).finally(() => {
        conversationsRefetchRef.current = false;
        if (conversationsRefetchDirtyRef.current) {
          conversationsRefetchDirtyRef.current = false;
          refresh();
        }
      });
    };
    refresh();
  }, [clearLocalUnread, loadConversations]);

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
    const currentUser = user;
    let cached: CachedMessage[] = [];
    let directComplete = true;
    let watermark = 0;
    let outbox: CachedOutbox[] = [];
    if (username !== "") {
      const [stored, syncState, pending] = await Promise.all([
        readMessages(username, target.key),
        readSyncState(username),
        readOutbox(username, target.key),
      ]);
      cached = stored;
      directComplete = syncState.directComplete;
      watermark = syncState.localSeq[target.key] ?? 0;
      outbox = pending;
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
      watermark = 0;
    }
    if (cached.length === 0 && watermark > 0 && username !== "") {
      // A watermark with no cached messages behind it cannot be compared.
      void removeMessages(username, target.key);
      watermark = 0;
    }
    const failedOutbox = (confirmed: Iterable<string>): DisplayMessage[] =>
      currentUser === null ? [] : outboxFailedMessages(outbox, confirmed, currentUser);
    if (cached.length > 0) {
      setMessages([
        ...cached.map(toDisplayMessage),
        ...failedOutbox(new Set(cached.map((message) => message.client_message_id))),
      ]);
      setMessagesLoading(false);
      setHasMore(true);
      scrollToBottomNextRef.current = true;
    }
    if (offlineRef.current) {
      setMessagesLoading(false);
      if (cached.length === 0) {
        setMessages(failedOutbox(new Set()));
        if (outbox.length === 0) {
          setMessagesError("当前离线，这个会话没有缓存。");
        }
      }
      return;
    }

    try {
      let localMax = maxAcceptedSeq(cached);
      const knownLast = conversationLastSeqsRef.current.get(target.key);
      const rollback = localMax > 0
        ? planListRollback(knownLast, localMax)
        : null;
      if (rollback !== null) {
        // Server counter went backwards: drop the reusable tail and reload.
        let archived: CachedOutbox[] = [];
        if (username !== "") {
          archived = await deleteMessagesFrom(
            username,
            target.key,
            rollback.fromSeq,
          );
        }
        cached = cached.filter((message) => message.seq < rollback.fromSeq);
        localMax = rollback.fromSeq - 1;
        missingSeqTrackerRef.current.clear(target.key);
        outbox = mergeOutboxEntries(outbox, archived);
        setMessages((previous) => previous.filter((message) =>
          !isAcceptedSeq(message.seq) || message.seq <= localMax));
        showArchivedFailures(target, archived);
      }

      const serverLast = conversationLastMessagesRef.current.get(target.key);
      const cachedNewest = cached.length > 0 ? cached[cached.length - 1] : null;
      if (
        serverLast !== undefined
        && cachedNewest !== null
        && cachedNewest.seq === serverLast.seq
        && cachedNewest.id !== serverLast.id
      ) {
        // Same seq, different global id: the server reused the tail.
        let archived: CachedOutbox[] = [];
        if (username !== "") {
          archived = await deleteMessagesFrom(
            username,
            target.key,
            serverLast.seq,
          );
        }
        cached = cached.filter((message) => message.seq < serverLast.seq);
        localMax = serverLast.seq - 1;
        missingSeqTrackerRef.current.clear(target.key);
        outbox = mergeOutboxEntries(outbox, archived);
        setMessages((previous) => previous.filter((message) =>
          !isAcceptedSeq(message.seq) || message.seq <= localMax));
        showArchivedFailures(target, archived);
      }

      if (localMax === 0 || cached.length === 0) {
        const page = await fetchInitialPage(target);
        if (!isCurrent()) {
          return;
        }
        const confirmed = new Set(
          page.messages.map((message) => message.client_message_id),
        );
        setMessages([...page.messages, ...failedOutbox(confirmed)]);
        setHasMore(page.hasMore);
        scrollToBottomNextRef.current = true;
        if (username !== "") {
          void writeMessages(username, target.key, page.messages);
        }
      } else if (knownLast === undefined || knownLast > localMax) {
        const pulled = await collectAfterSeq(target, localMax);
        if (!isCurrent()) {
          return;
        }
        if (pulled.truncated) {
          const page = await fetchInitialPage(target);
          if (!isCurrent()) {
            return;
          }
          const confirmed = new Set(
            page.messages.map((message) => message.client_message_id),
          );
          setMessages([...page.messages, ...failedOutbox(confirmed)]);
          setHasMore(page.hasMore);
          if (username !== "") {
            void writeMessages(username, target.key, page.messages);
          }
        } else {
          const present = new Set(pulled.messages.map((message) => message.seq));
          setMessages((previous) => mergeMessages(previous, pulled.messages));
          if (username !== "") {
            void writeMessages(username, target.key, pulled.messages);
          }
          const gapMessages = await pullGaps(
            target,
            localMax,
            Math.max(pulled.lastSeq, knownLast ?? 0),
            present,
            missingSeqTrackerRef.current,
          );
          if (!isCurrent()) {
            return;
          }
          if (gapMessages.length > 0) {
            setMessages((previous) => mergeMessages(previous, gapMessages));
            if (username !== "") {
              void writeMessages(username, target.key, gapMessages);
            }
          }
        }
        if (nearBottomRef.current) {
          scrollToBottomNextRef.current = true;
        }
      } else if (cached.length > 1) {
        // Cached pages can have holes of their own (a lost push that the
        // server later skipped). Fill them once; leftovers stay marked.
        const seqs = cached
          .filter((message) => isAcceptedSeq(message.seq))
          .map((message) => message.seq);
        const minSeq = Math.min(...seqs);
        const maxSeq = Math.max(...seqs);
        if (seqs.length > 1 && maxSeq - minSeq + 1 <= MAX_GAP_SCAN) {
          const present = new Set(seqs);
          const gapMessages = await pullGaps(
            target,
            minSeq - 1,
            maxSeq,
            present,
            missingSeqTrackerRef.current,
          );
          if (isCurrent() && gapMessages.length > 0) {
            setMessages((previous) => mergeMessages(previous, gapMessages));
            if (username !== "") {
              void writeMessages(username, target.key, gapMessages);
            }
          }
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
    showArchivedFailures,
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
  const peerLastReadSeq = activeDirect?.peer_last_read_seq ?? 0;
  const lastOwnSentSeq = useMemo(() => {
    if (!user || !messagesBelongToActive) {
      return 0;
    }
    return findLastOwnSentSeq(messages, user.username);
  }, [messages, messagesBelongToActive, user]);

  useEffect(() => {
    const target = activeTargetRef.current;
    const socket = websocketRef.current;
    if (target !== null && (socket === null || socket.readyState !== WebSocket.OPEN)) {
      // Opening a conversation is a read even without a socket; remember it so
      // the reconnect can focus it and let the server catch the position up.
      const key = focusTargetKey(target);
      pendingFocusReadsRef.current.set(key, target);
      clearLocalUnread(
        target,
        conversationLastSeqsRef.current.get(key) ?? 0,
      );
      return;
    }
    syncFocus();
  }, [activeKey, clearLocalUnread, syncFocus]);

  useEffect(() => {
    syncFocus();
  }, [pageVisible, syncFocus]);

  useEffect(() => {
    readStatsDebouncerRef.current.cancel();
    readStatsGateRef.current.reset();
    const target = activeGroupID > 0 && messagesBelongToActive
      && lastOwnSentSeq > 0
      ? { groupID: activeGroupID, seq: lastOwnSentSeq }
      : null;
    groupReceiptTargetRef.current = target;
    setGroupReceipt(null);
    if (target !== null) {
      void fetchGroupReadStats();
    }
  }, [
    activeGroupID,
    fetchGroupReadStats,
    lastOwnSentSeq,
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
    const oldest = messages.find((message) => isAcceptedSeq(message.seq));
    if (!oldest) {
      return;
    }
    const version = conversationVersionRef.current;
    const container = messageListRef.current;
    const previousHeight = container?.scrollHeight ?? 0;
    const previousTop = container?.scrollTop ?? 0;
    setLoadingOlder(true);
    try {
      const page = await fetchOlderPage(target, oldest.seq);
      if (version !== conversationVersionRef.current) {
        return;
      }
      pendingOlderScrollRef.current = { height: previousHeight, top: previousTop };
      setMessages((previous) => mergeMessages(previous, page.messages));
      if (user) {
        void writeMessages(user.username, target.key, page.messages);
      }
      if (!page.hasMore) {
        setHasMore(false);
      } else {
        setHasMore(true);
      }
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
    const key = directKey(otherUser.username);
    void writeMessages(currentUser.username, key, [message]);
    const target = activeTargetRef.current;
    const targetMatches = target?.kind === "direct"
      && sameUsername(target.username, otherUser.username);
    const readingNow = targetMatches
      && pageVisibleRef.current
      && !replacedRef.current;

    const knownLast = conversationLastSeqsRef.current.get(key) ?? 0;
    const openMessages = targetMatches
      ? messagesRef.current.filter((item) =>
        !isGroupMessage(item)
        && (sameUsername(item.sender.username, otherUser.username)
          || sameUsername(item.recipient.username, otherUser.username)))
      : [];
    const seqIDs = targetMatches ? acceptedSeqMap(openMessages) : undefined;
    const listPlan = planIncoming(message.seq, message.id, {
      maxSeq: knownLast,
      seqIDs,
    });
    if (!conversationKeysRef.current.has(key)) {
      void loadConversations(false);
    } else if (listPlan.kind === "pull" || listPlan.kind === "resync") {
      // A gap or a reused seq means the server list is the only safe source
      // for unread and the preview.
      void loadConversations(false);
    } else if (listPlan.kind === "append") {
      const unread = nextUnread(listPlan, 0, !incoming);
      conversationLastSeqsRef.current.set(
        key,
        Math.max(knownLast, message.seq),
      );
      setConversations((previous) => sortConversations(previous.map((conversation) =>
        conversation.type === "direct"
          && sameUsername(conversation.user.username, otherUser.username)
          ? {
            ...conversation,
            last_message: shouldReplacePreview(conversation.last_seq, message.seq)
              ? message
              : conversation.last_message,
            last_seq: Math.max(conversation.last_seq, message.seq),
            unread_count: readingNow && incoming
              ? 0
              : conversation.unread_count + unread.unread,
          }
          : conversation)));
    }

    if (!target || !targetMatches) {
      return;
    }
    const openPlan = planIncoming(message.seq, message.id, {
      maxSeq: maxAcceptedSeq(openMessages),
      seqIDs: acceptedSeqMap(openMessages),
    });
    if (openPlan.kind === "duplicate") {
      return;
    }
    const wasNearBottom = nearBottomRef.current;
    if (openPlan.kind === "append") {
      setMessages((previous) => mergeMessages(previous, [toDirectDisplay(message)]));
    } else if (openPlan.kind === "pull") {
      setMessages((previous) => mergeMessages(previous, [toDirectDisplay(message)]));
      const present = new Set(
        openMessages
          .filter((item) => isAcceptedSeq(item.seq))
          .map((item) => item.seq),
      );
      void (async () => {
        try {
          const gapMessages = await pullGaps(
            target,
            openPlan.afterSeq,
            openPlan.targetSeq - 1,
            present,
            missingSeqTrackerRef.current,
          );
          if (gapMessages.length === 0) {
            return;
          }
          setMessages((previous) => mergeMessages(previous, gapMessages));
          if (userRef.current) {
            void writeMessages(userRef.current.username, key, gapMessages);
          }
        } catch {
          // The conversation refresh above already scheduled a retry path.
        }
      })();
    } else {
      missingSeqTrackerRef.current.clear(key);
      // Seqs above the reused one are stale; the pushed message itself is
      // authoritative. It stays out of the delete range and is re-written after
      // the delete so an aborted resync cannot drop it from the cache.
      void deleteMessagesFrom(currentUser.username, key, openPlan.fromSeq + 1)
        .then((archived) => {
          showArchivedFailures(target, archived);
          return writeMessages(currentUser.username, key, [message]);
        });
      setMessages((previous) => mergeMessages(previous, [toDirectDisplay(message)])
        .filter((item) =>
          !isAcceptedSeq(item.seq) || item.seq <= openPlan.fromSeq));
      resyncTailRef.current(target, openPlan.fromSeq);
    }
    if (wasNearBottom) {
      scrollToBottomNextRef.current = true;
    }
  }, [loadConversations, showArchivedFailures]);

  const handleIncomingGroupMessage = useCallback((message: GroupMessage) => {
    const currentUser = userRef.current;
    if (!currentUser) {
      return;
    }
    const sentByCurrent = sameUsername(message.sender.username, currentUser.username);
    const target = activeTargetRef.current;
    const targetMatches = target?.kind === "group"
      && target.groupID === message.group_id;
    const readingNow = targetMatches
      && pageVisibleRef.current
      && !replacedRef.current;
    const key = groupKey(message.group_id);
    void writeMessages(currentUser.username, key, [message]);

    const knownLast = conversationLastSeqsRef.current.get(key) ?? 0;
    const openMessages = targetMatches
      ? messagesRef.current.filter((item) =>
        isGroupMessage(item) && item.group_id === message.group_id)
      : [];
    const seqIDs = targetMatches ? acceptedSeqMap(openMessages) : undefined;
    const listPlan = planIncoming(message.seq, message.id, {
      maxSeq: knownLast,
      seqIDs,
    });
    if (!conversationKeysRef.current.has(key)) {
      void loadConversations(false);
    } else if (listPlan.kind === "pull" || listPlan.kind === "resync") {
      void loadConversations(false);
    } else if (listPlan.kind === "append") {
      const unread = nextUnread(listPlan, 0, sentByCurrent);
      conversationLastSeqsRef.current.set(
        key,
        Math.max(knownLast, message.seq),
      );
      setConversations((previous) => sortConversations(previous.map((conversation) =>
        conversation.type === "group" && conversation.group.id === message.group_id
          ? {
            ...conversation,
            last_message: shouldReplacePreview(conversation.last_seq, message.seq)
              ? message
              : conversation.last_message,
            last_seq: Math.max(conversation.last_seq, message.seq),
            unread_count: readingNow && !sentByCurrent
              ? 0
              : conversation.unread_count + unread.unread,
          }
          : conversation)));
    }

    if (!target || !targetMatches) {
      return;
    }
    const openPlan = planIncoming(message.seq, message.id, {
      maxSeq: maxAcceptedSeq(openMessages),
      seqIDs: acceptedSeqMap(openMessages),
    });
    if (openPlan.kind === "duplicate") {
      return;
    }
    const wasNearBottom = nearBottomRef.current;
    if (openPlan.kind === "append") {
      setMessages((previous) => mergeMessages(previous, [toGroupDisplay(message)]));
    } else if (openPlan.kind === "pull") {
      setMessages((previous) => mergeMessages(previous, [toGroupDisplay(message)]));
      const present = new Set(
        openMessages
          .filter((item) => isAcceptedSeq(item.seq))
          .map((item) => item.seq),
      );
      void (async () => {
        try {
          const gapMessages = await pullGaps(
            target,
            openPlan.afterSeq,
            openPlan.targetSeq - 1,
            present,
            missingSeqTrackerRef.current,
          );
          if (gapMessages.length === 0) {
            return;
          }
          setMessages((previous) => mergeMessages(previous, gapMessages));
          if (userRef.current) {
            void writeMessages(userRef.current.username, key, gapMessages);
          }
        } catch {
          // The conversation refresh above already scheduled a retry path.
        }
      })();
    } else {
      missingSeqTrackerRef.current.clear(key);
      // Seqs above the reused one are stale; the pushed message itself is
      // authoritative. It stays out of the delete range and is re-written after
      // the delete so an aborted resync cannot drop it from the cache.
      void deleteMessagesFrom(currentUser.username, key, openPlan.fromSeq + 1)
        .then((archived) => {
          showArchivedFailures(target, archived);
          return writeMessages(currentUser.username, key, [message]);
        });
      setMessages((previous) => mergeMessages(previous, [toGroupDisplay(message)])
        .filter((item) =>
          !isAcceptedSeq(item.seq) || item.seq <= openPlan.fromSeq));
      resyncTailRef.current(target, openPlan.fromSeq);
    }
    if (wasNearBottom) {
      scrollToBottomNextRef.current = true;
    }
  }, [loadConversations, showArchivedFailures]);

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
          && conversation.last_seq <= event.lastReadSeq
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
            peer_last_read_seq: Math.max(
              conversation.peer_last_read_seq ?? 0,
              event.lastReadSeq,
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
    const ownMessages = messagesRef.current.filter((message) => {
      if (target.kind === "group") {
        return isGroupMessage(message) && message.group_id === target.groupID;
      }
      return !isGroupMessage(message)
        && (sameUsername(message.sender.username, target.username)
          || sameUsername(message.recipient.username, target.username));
    });
    const newest = maxAcceptedSeq(ownMessages);

    syncingRef.current = true;
    try {
      // The list snapshot only identifies a reused tail message here; a stale
      // last_seq must not be read as a rollback (loadConversations checks that
      // against a fresh list response).
      const knownLast = conversationLastSeqsRef.current.get(target.key) ?? 0;
      const serverLast = conversationLastMessagesRef.current.get(target.key);
      const newestMessage = ownMessages.reduce<DisplayMessage | undefined>(
        (latest, message) => (message.seq === newest ? message : latest),
        undefined,
      );
      const plan = planActiveSync(newest, newestMessage, serverLast);
      if (plan.kind === "resync") {
        let archived: CachedOutbox[] = [];
        if (user) {
          archived = await deleteMessagesFrom(
            user.username,
            target.key,
            plan.fromSeq,
          );
        }
        setMessages((previous) => previous.filter((message) =>
          !isAcceptedSeq(message.seq) || message.seq < plan.fromSeq));
        showArchivedFailures(target, archived);
        resyncTailRef.current(target, plan.fromSeq - 1);
        return;
      }

      if (plan.kind === "initial") {
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
        const pulled = await collectAfterSeq(target, plan.afterSeq);
        if (!isCurrent()) {
          return;
        }
        if (pulled.truncated && isCurrent()) {
          await loadInitialMessages(target);
          return;
        }
        if (pulled.messages.length > 0) {
          setMessages((previous) => mergeMessages(previous, pulled.messages));
          if (user) {
            void writeMessages(user.username, target.key, pulled.messages);
          }
        }
        const present = new Set(pulled.messages.map((message) => message.seq));
        const gapMessages = await pullGaps(
          target,
          newest,
          Math.max(pulled.lastSeq, knownLast),
          present,
          missingSeqTrackerRef.current,
        );
        if (!isCurrent()) {
          return;
        }
        if (gapMessages.length > 0) {
          setMessages((previous) => mergeMessages(previous, gapMessages));
          if (user) {
            void writeMessages(user.username, target.key, gapMessages);
          }
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
    showArchivedFailures,
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
    inFlightSends.add(message.client_message_id);
    try {
      const raw = target.kind === "direct"
        ? (await api.sendMessage(
          target.username,
          message.body,
          message.client_message_id,
        )).message
        : (await api.sendGroupMessage(
          target.groupID,
          message.body,
          message.client_message_id,
        )).message;
      if (raw.id <= 0 || !isAcceptedSeq(raw.seq)) {
        throw new ApiError(0, "invalid_message", "服务器返回的消息号无效。");
      }
      const sent = "group_id" in raw
        ? toGroupDisplay(raw)
        : toDirectDisplay(raw);
      if (user) {
        void writeMessages(user.username, target.key, [sent]);
        void deleteOutbox(
          user.username,
          target.key,
          message.client_message_id,
        );
      }
      if (isCurrent()) {
        setMessages((previous) => mergeMessages(previous, [sent]));
        setSendError(null);
        composerRef.current?.focus();
      }
      if (conversationKeysRef.current.has(target.key)) {
        setConversations((previous) => sortConversations(previous.map((conversation) =>
          conversationKey(conversation) === target.key
            ? {
              ...conversation,
              last_message: shouldReplacePreview(conversation.last_seq, sent.seq)
                ? sent
                : conversation.last_message,
              last_seq: Math.max(conversation.last_seq, sent.seq),
            }
            : conversation)));
      } else {
        void loadConversations(false);
      }
      // Sending means the sender has read up to this message. The server's echo
      // only arrives over the socket, so clear locally when it is down.
      clearLocalUnread(target, sent.seq);
    } catch (error) {
      if (handleSessionError(error)) {
        if (user) {
          void markOutboxFailed(
            user.username,
            target.key,
            message.client_message_id,
          );
        }
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
      if (user) {
        void markOutboxFailed(
          user.username,
          target.key,
          message.client_message_id,
        );
      }
      if (isCurrent()) {
        setMessages((previous) => previous.map((item) =>
          item.client_message_id === message.client_message_id
            ? { ...item, status: "failed" }
            : item));
        setSendError(describeError(error));
      }
    } finally {
      inFlightSends.delete(message.client_message_id);
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

    const createdAt = new Date().toISOString();
    const clientMessageID = newClientMessageID();
    const base = {
      id: 0,
      seq: 0,
      sender: { username: user.username, nickname: user.nickname },
      body,
      client_message_id: clientMessageID,
      created_at: createdAt,
      status: "sending" as const,
    };
    const message: DisplayMessage = target.kind === "direct"
      ? { ...base, recipient: activeDirect!.user }
      : { ...base, group_id: target.groupID };
    const outbox: CachedOutbox = target.kind === "direct"
      ? {
        user: user.username,
        conversation: target.key,
        client_message_id: clientMessageID,
        body,
        kind: "direct",
        recipient: activeDirect!.user,
        created_at: createdAt,
        status: "sending",
      }
      : {
        user: user.username,
        conversation: target.key,
        client_message_id: clientMessageID,
        body,
        kind: "group",
        group_id: target.groupID,
        created_at: createdAt,
        status: "sending",
      };
    await writeOutbox(outbox);
    setMessages((previous) => mergeMessages(previous, [message]));
    setDraft("");
    setDraftError(null);
    setSendError(null);
    scrollToBottomNextRef.current = true;
    await deliverMessage(message, target);
  }

  async function retryMessage(message: DisplayMessage) {
    const target = activeTarget;
    if (!target || !user || isReadOnly || isReplaced) {
      return;
    }
    const outbox: CachedOutbox = isGroupMessage(message)
      ? {
        user: user.username,
        conversation: target.key,
        client_message_id: message.client_message_id,
        body: message.body,
        kind: "group",
        group_id: message.group_id,
        created_at: message.created_at,
        status: "sending",
      }
      : {
        user: user.username,
        conversation: target.key,
        client_message_id: message.client_message_id,
        body: message.body,
        kind: "direct",
        recipient: message.recipient,
        created_at: message.created_at,
        status: "sending",
      };
    await writeOutbox(outbox);
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
        last_seq: 0,
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
                              data-seq={message.seq || undefined}
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
                                    && isAcceptedSeq(message.seq)
                                    && message.seq === lastOwnSentSeq && (
                                      <span className={`read-state${isReadByPeer(message.seq, peerLastReadSeq) ? " read-state--read" : ""}`}>
                                        {isReadByPeer(message.seq, peerLastReadSeq) ? "已读" : "未读"}
                                      </span>
                                    )}
                                  {mine
                                    && groupMessage
                                    && message.status === "sent"
                                    && isAcceptedSeq(message.seq)
                                    && message.seq === lastOwnSentSeq
                                    && groupReceipt?.groupID === message.group_id
                                    && groupReceipt.seq === message.seq
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
