import { isAcceptedSeq } from "../api";
import type { Conversation, PublicUser, User } from "../api";
import {
  CONVERSATIONS_STORE,
  FRIENDS_STORE,
  MESSAGES_SEQ_INDEX,
  MESSAGES_STORE,
  META_STORE,
  OUTBOX_STORE,
  closeCacheDB,
  openCacheDB,
  requestToPromise,
  transactionDone,
} from "./db";
import type { CacheStoreName } from "./db";
import { conversationKey, groupIDFromKey } from "./keys";
import { mergeMessages, sortMessagesBySeq } from "./merge";
import {
  CONVERSATION_CACHE_LIMIT,
  MESSAGE_LIMIT_PER_CONVERSATION,
  MESSAGE_LIMIT_PER_USER,
  emptySyncState,
} from "./types";
import type {
  CacheHints,
  CachedConversation,
  CachedFriends,
  CachedMessage,
  CachedOutbox,
  SyncState,
} from "./types";

const ACTIVE_USER_KEY = "active-user";

type CachedMessageRecord = {
  user: string;
  conversation: string;
  id: number;
  seq: number;
  message: CachedMessage;
};

let disabled = false;
let warned = false;

function syncKey(user: string): string {
  return `${user}:sync`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function readSyncStateValue(value: unknown): SyncState | null {
  if (!isRecord(value)) {
    return null;
  }
  const { afterID, directComplete, groups, localSeq, savedAt } = value;
  if (
    typeof afterID !== "number" ||
    !Number.isInteger(afterID) ||
    afterID < 0 ||
    typeof directComplete !== "boolean" ||
    !isRecord(groups) ||
    !isRecord(localSeq) ||
    typeof savedAt !== "string"
  ) {
    return null;
  }

  // Group cursor values are seq units; afterID above is the private feed's
  // global message id. Never compare the two.
  const cursors: Record<number, number> = {};
  for (const [key, cursor] of Object.entries(groups)) {
    const groupID = Number(key);
    if (
      Number.isInteger(groupID) &&
      groupID > 0 &&
      typeof cursor === "number" &&
      Number.isInteger(cursor) &&
      cursor >= 0
    ) {
      cursors[groupID] = cursor;
    }
  }

  const seqs: Record<string, number> = {};
  for (const [key, seq] of Object.entries(localSeq)) {
    if (key === "") {
      continue;
    }
    if (typeof seq === "number" && Number.isInteger(seq) && seq >= 0) {
      seqs[key] = seq;
    }
  }

  return { afterID, directComplete, groups: cursors, localSeq: seqs, savedAt };
}

function readUserValue(value: unknown): User | null {
  if (!isRecord(value)) {
    return null;
  }
  if (typeof value.username !== "string" || value.username === "") {
    return null;
  }
  if (typeof value.nickname !== "string") {
    return null;
  }
  return { username: value.username, nickname: value.nickname };
}

function readFriendsValue(value: unknown): PublicUser[] | null {
  if (!isRecord(value) || !Array.isArray(value.friends)) {
    return null;
  }
  const friends: PublicUser[] = [];
  for (const entry of value.friends) {
    if (
      isRecord(entry) &&
      typeof entry.username === "string" &&
      typeof entry.nickname === "string"
    ) {
      friends.push({ username: entry.username, nickname: entry.nickname });
    }
  }
  return friends;
}

function isValidConversationRecord(
  value: unknown,
): value is CachedConversation {
  if (!isRecord(value)) {
    return false;
  }
  if (typeof value.user !== "string" || typeof value.key !== "string") {
    return false;
  }
  if (typeof value.updatedAt !== "number") {
    return false;
  }
  const conversation = value.conversation;
  if (!isRecord(conversation)) {
    return false;
  }
  if (
    typeof conversation.last_seq !== "number"
    || !Number.isInteger(conversation.last_seq)
    || conversation.last_seq < 0
  ) {
    return false;
  }
  const lastMessage = conversation.last_message;
  if (
    lastMessage !== null
    && (!isRecord(lastMessage)
      || typeof lastMessage.id !== "number"
      || lastMessage.id <= 0
      || !isAcceptedSeq(lastMessage.seq))
  ) {
    return false;
  }
  if (conversation.type === "direct") {
    const peerSeq = conversation.peer_last_read_seq;
    return isRecord(conversation.user)
      && (peerSeq === undefined
        || (typeof peerSeq === "number"
          && Number.isInteger(peerSeq)
          && peerSeq >= 0));
  }
  if (conversation.type === "group") {
    return isRecord(conversation.group);
  }
  return false;
}

function isValidMessageRecord(value: unknown): value is CachedMessageRecord {
  if (!isRecord(value)) {
    return false;
  }
  if (typeof value.user !== "string" || typeof value.conversation !== "string") {
    return false;
  }
  if (typeof value.id !== "number" || value.id <= 0) {
    return false;
  }
  if (!isAcceptedSeq(value.seq)) {
    return false;
  }
  if (!isRecord(value.message)) {
    return false;
  }
  return value.message.id === value.id && value.message.seq === value.seq;
}

function isValidOutboxRecord(value: unknown): value is CachedOutbox {
  if (!isRecord(value)) {
    return false;
  }
  if (
    typeof value.user !== "string"
    || value.user === ""
    || typeof value.conversation !== "string"
    || value.conversation === ""
    || typeof value.client_message_id !== "string"
    || value.client_message_id === ""
    || typeof value.body !== "string"
    || typeof value.created_at !== "string"
    || (value.status !== "sending" && value.status !== "failed")
  ) {
    return false;
  }
  if (value.kind === "direct") {
    const recipient = value.recipient;
    return isRecord(recipient)
      && typeof recipient.username === "string"
      && typeof recipient.nickname === "string";
  }
  if (value.kind === "group") {
    return typeof value.group_id === "number"
      && Number.isInteger(value.group_id)
      && value.group_id > 0;
  }
  return false;
}

function readValue(
  store: IDBObjectStore,
  key: IDBValidKey,
): Promise<unknown> {
  return requestToPromise(store.get(key) as IDBRequest<unknown>);
}

function readAll<T>(store: IDBObjectStore, query?: IDBKeyRange): Promise<T[]> {
  const request = query === undefined ? store.getAll() : store.getAll(query);
  return requestToPromise(request as IDBRequest<T[]>);
}

function conversationMessagesRange(
  user: string,
  conversation: string,
): IDBKeyRange {
  return IDBKeyRange.bound(
    [user, conversation, 0],
    [user, conversation, Number.MAX_SAFE_INTEGER],
  );
}

function userMessagesRange(user: string): IDBKeyRange {
  // [user, []] sorts after every [user, conversation, id] key.
  return IDBKeyRange.bound([user], [user, []]);
}

function normalizeLimit(value: number | undefined, fallback: number): number {
  if (value === undefined || !Number.isFinite(value)) {
    return fallback;
  }
  return Math.max(0, Math.floor(value));
}

function normalizeCursor(value: number | undefined, fallback: number): number {
  if (value === undefined || !Number.isFinite(value) || value < 0) {
    return fallback;
  }
  return Math.floor(value);
}

function mergeGroupCursors(
  current: Record<number, number>,
  incoming: Record<number, number>,
): Record<number, number> {
  const merged = { ...current };
  for (const [key, cursor] of Object.entries(incoming)) {
    const groupID = Number(key);
    if (
      Number.isInteger(groupID) &&
      groupID > 0 &&
      Number.isInteger(cursor) &&
      cursor > 0
    ) {
      merged[groupID] = Math.max(merged[groupID] ?? 0, cursor);
    }
  }
  return merged;
}

function mergeLocalSeqs(
  current: Record<string, number>,
  incoming: Record<string, number>,
): Record<string, number> {
  const merged = { ...current };
  for (const [key, seq] of Object.entries(incoming)) {
    if (key !== "" && Number.isInteger(seq) && seq > 0) {
      merged[key] = Math.max(merged[key] ?? 0, seq);
    }
  }
  return merged;
}

function conversationSeqRange(
  user: string,
  conversation: string,
  fromSeq: number,
): IDBKeyRange {
  return IDBKeyRange.bound(
    [user, conversation, fromSeq],
    [user, conversation, Number.MAX_SAFE_INTEGER],
  );
}

function conversationOutboxRange(
  user: string,
  conversation: string,
): IDBKeyRange {
  return IDBKeyRange.bound([user, conversation], [user, conversation, []]);
}

function disableCache(context: string, error: unknown): void {
  disabled = true;
  if (warned) {
    return;
  }
  warned = true;
  console.warn(
    `[cache] IndexedDB is unavailable (${context}); caching is disabled for this session.`,
    error,
  );
}

function shouldSkipCache(): boolean {
  if (disabled) {
    return true;
  }
  if (!cacheSupported()) {
    disableCache("unsupported", null);
    return true;
  }
  return false;
}

async function runRead<T>(
  fallback: T,
  operation: (db: IDBDatabase) => Promise<T>,
): Promise<T> {
  if (shouldSkipCache()) {
    return fallback;
  }
  try {
    const db = await openCacheDB();
    return await operation(db);
  } catch (error) {
    disableCache("read", error);
    return fallback;
  }
}

async function runWrite(
  operation: (db: IDBDatabase) => Promise<void>,
): Promise<void> {
  if (shouldSkipCache()) {
    return;
  }
  try {
    const db = await openCacheDB();
    await operation(db);
  } catch (error) {
    disableCache("write", error);
  }
}

function runTransaction<T>(
  db: IDBDatabase,
  stores: CacheStoreName[],
  mode: IDBTransactionMode,
  operation: (transaction: IDBTransaction) => Promise<T>,
): Promise<T> {
  const transaction = db.transaction(stores, mode);
  const done = transactionDone(transaction);

  return operation(transaction).then(
    async (result) => {
      await done;
      return result;
    },
    async (error: unknown) => {
      try {
        transaction.abort();
      } catch {
        // The transaction already finished.
      }
      await done.catch(() => undefined);
      throw error;
    },
  );
}

async function updateSyncState(
  db: IDBDatabase,
  user: string,
  update: (current: SyncState) => SyncState,
): Promise<void> {
  await runTransaction(db, [META_STORE], "readwrite", async (transaction) => {
    const store = transaction.objectStore(META_STORE);
    const value = await readValue(store, syncKey(user));
    const current = readSyncStateValue(value) ?? emptySyncState();
    await requestToPromise(store.put(update(current), syncKey(user)));
  });
}

export function cacheSupported(): boolean {
  return typeof indexedDB !== "undefined";
}

export async function resetCacheForTests(): Promise<void> {
  await closeCacheDB();
  disabled = false;
  warned = false;
}

export async function readHints(): Promise<CacheHints> {
  return runRead<CacheHints>(
    { user: null, sync: null },
    async (db) =>
      runTransaction(db, [META_STORE], "readonly", async (transaction) => {
        const store = transaction.objectStore(META_STORE);
        const userValue = readUserValue(await readValue(store, ACTIVE_USER_KEY));
        if (userValue === null) {
          return { user: null, sync: null };
        }
        const syncValue = await readValue(store, syncKey(userValue.username));
        return { user: userValue, sync: readSyncStateValue(syncValue) };
      }),
  );
}

export async function setActiveUser(user: User): Promise<void> {
  await runWrite(async (db) => {
    await runTransaction(db, [META_STORE], "readwrite", async (transaction) => {
      await requestToPromise(
        transaction.objectStore(META_STORE).put(
          { username: user.username, nickname: user.nickname },
          ACTIVE_USER_KEY,
        ),
      );
    });
  });
}

export async function readConversations(
  user: string,
): Promise<Conversation[]> {
  return runRead<Conversation[]>(
    [],
    async (db) =>
      runTransaction(
        db,
        [CONVERSATIONS_STORE],
        "readonly",
        async (transaction) => {
          const records = await readAll<CachedConversation>(
            transaction.objectStore(CONVERSATIONS_STORE),
          );
          return records
            .filter(isValidConversationRecord)
            .filter((record) => record.user === user)
            .map((record) => record.conversation);
        },
      ),
  );
}

export async function writeConversations(
  user: string,
  conversations: Conversation[],
): Promise<void> {
  await runWrite(async (db) => {
    await runTransaction(
      db,
      [CONVERSATIONS_STORE, MESSAGES_STORE, META_STORE, OUTBOX_STORE],
      "readwrite",
      async (transaction) => {
        const conversationStore = transaction.objectStore(CONVERSATIONS_STORE);
        const messageStore = transaction.objectStore(MESSAGES_STORE);
        const metaStore = transaction.objectStore(META_STORE);
        const outboxStore = transaction.objectStore(OUTBOX_STORE);

        const records = await readAll<CachedConversation>(conversationStore);
        const existing = new Map<string, CachedConversation>();
        for (const record of records) {
          if (record.user === user) {
            existing.set(record.key, record);
          }
        }

        const keptKeys = new Set(
          conversations.map((conversation) => conversationKey(conversation)),
        );
        const removedGroups: number[] = [];
        const removedKeys: string[] = [];

        for (const key of existing.keys()) {
          if (keptKeys.has(key)) {
            continue;
          }
          await requestToPromise(conversationStore.delete([user, key]));
          await requestToPromise(
            messageStore.delete(conversationMessagesRange(user, key)),
          );
          await requestToPromise(
            outboxStore.delete(conversationOutboxRange(user, key)),
          );
          removedKeys.push(key);
          const groupID = groupIDFromKey(key);
          if (groupID !== null) {
            removedGroups.push(groupID);
          }
        }

        const now = Date.now();
        for (const conversation of conversations) {
          const key = conversationKey(conversation);
          const record: CachedConversation = {
            user,
            key,
            conversation,
            updatedAt: now,
          };
          await requestToPromise(conversationStore.put(record));
        }

        if (removedKeys.length > 0) {
          const state = readSyncStateValue(
            await readValue(metaStore, syncKey(user)),
          );
          if (state !== null) {
            let changed = false;
            for (const groupID of removedGroups) {
              if (groupID in state.groups) {
                delete state.groups[groupID];
                changed = true;
              }
            }
            for (const key of removedKeys) {
              if (key in state.localSeq) {
                delete state.localSeq[key];
                changed = true;
              }
            }
            if (changed) {
              await requestToPromise(
                metaStore.put(state, syncKey(user)),
              );
            }
          }
        }
      },
    );
  });
}

export async function readFriends(user: string): Promise<PublicUser[]> {
  return runRead<PublicUser[]>(
    [],
    async (db) =>
      runTransaction(db, [FRIENDS_STORE], "readonly", async (transaction) => {
        const value = await readValue(
          transaction.objectStore(FRIENDS_STORE),
          [user],
        );
        return readFriendsValue(value) ?? [];
      }),
  );
}

export async function writeFriends(
  user: string,
  friends: PublicUser[],
): Promise<void> {
  await runWrite(async (db) => {
    await runTransaction(db, [FRIENDS_STORE], "readwrite", async (transaction) => {
      const record: CachedFriends = {
        user,
        friends,
        updatedAt: Date.now(),
      };
      await requestToPromise(
        transaction.objectStore(FRIENDS_STORE).put(record),
      );
    });
  });
}

export async function readMessages(
  user: string,
  conversation: string,
  limit = MESSAGE_LIMIT_PER_CONVERSATION,
): Promise<CachedMessage[]> {
  const effectiveLimit = normalizeLimit(limit, MESSAGE_LIMIT_PER_CONVERSATION);
  return runRead<CachedMessage[]>(
    [],
    async (db) =>
      runTransaction(db, [MESSAGES_STORE], "readonly", async (transaction) => {
        const records = await readAll<CachedMessageRecord>(
          transaction.objectStore(MESSAGES_STORE),
          conversationMessagesRange(user, conversation),
        );
        const sorted = sortMessagesBySeq(
          records.filter(isValidMessageRecord).map((record) => record.message),
        );
        const deduped: CachedMessage[] = [];
        for (const message of sorted) {
          if (
            deduped.length > 0
            && deduped[deduped.length - 1].seq === message.seq
          ) {
            deduped[deduped.length - 1] = message;
            continue;
          }
          deduped.push(message);
        }
        if (deduped.length <= effectiveLimit) {
          return deduped;
        }
        return deduped.slice(deduped.length - effectiveLimit);
      }),
  );
}

export async function readConversationSeq(
  user: string,
  conversation: string,
): Promise<number> {
  return runRead<number>(
    0,
    async (db) =>
      runTransaction(db, [META_STORE], "readonly", async (transaction) => {
        const state = readSyncStateValue(
          await readValue(transaction.objectStore(META_STORE), syncKey(user)),
        );
        return state?.localSeq[conversation] ?? 0;
      }),
  );
}

// Reads the highest seq actually stored through the (conversation, seq) index.
export async function readMaxMessageSeq(
  user: string,
  conversation: string,
): Promise<number> {
  return runRead<number>(
    0,
    async (db) =>
      runTransaction(db, [MESSAGES_STORE], "readonly", async (transaction) => {
        const index = transaction
          .objectStore(MESSAGES_STORE)
          .index(MESSAGES_SEQ_INDEX);
        const records = await requestToPromise(
          index.getAll(conversationSeqRange(user, conversation, 1)),
        );
        let max = 0;
        for (const record of records) {
          if (isValidMessageRecord(record) && record.seq > max) {
            max = record.seq;
          }
        }
        return max;
      }),
  );
}

export async function writeMessages(
  user: string,
  conversation: string,
  messages: CachedMessage[],
): Promise<void> {
  const confirmed = messages.filter(
    (message) =>
      Number.isInteger(message.id)
      && message.id > 0
      && isAcceptedSeq(message.seq),
  );
  if (confirmed.length === 0) {
    return;
  }
  await runWrite(async (db) => {
    await runTransaction(
      db,
      [MESSAGES_STORE, META_STORE, OUTBOX_STORE],
      "readwrite",
      async (transaction) => {
        const store = transaction.objectStore(MESSAGES_STORE);
        const metaStore = transaction.objectStore(META_STORE);
        const outboxStore = transaction.objectStore(OUTBOX_STORE);
        const records = await readAll<CachedMessageRecord>(
          store,
          conversationMessagesRange(user, conversation),
        );
        const merged = mergeMessages(
          records.filter(isValidMessageRecord).map((record) => record.message),
          confirmed,
        );
        // Ordering and trimming are by seq; the global id stays the key.
        const kept = merged.messages.slice(
          Math.max(0, merged.messages.length - MESSAGE_LIMIT_PER_CONVERSATION),
        );
        const keptIDs = new Set(kept.map((message) => message.id));
        for (const record of records) {
          if (!keptIDs.has(record.id)) {
            await requestToPromise(store.delete([user, conversation, record.id]));
          }
        }

        const incomingIDs = new Set(confirmed.map((message) => message.id));
        let newestSeq = 0;
        for (const message of kept) {
          if (message.seq > newestSeq) {
            newestSeq = message.seq;
          }
          if (!incomingIDs.has(message.id)) {
            continue;
          }
          const record: CachedMessageRecord = {
            user,
            conversation,
            id: message.id,
            seq: message.seq,
            message,
          };
          await requestToPromise(store.put(record));
        }

        if (newestSeq > 0) {
          const value = await readValue(metaStore, syncKey(user));
          const state = readSyncStateValue(value) ?? emptySyncState();
          const current = state.localSeq[conversation] ?? 0;
          // A reused seq rolled the timeline back, so the watermark has to
          // follow it down instead of staying monotonic.
          const nextSeq = merged.conflictSeq !== null
            ? newestSeq
            : Math.max(current, newestSeq);
          if (nextSeq !== current) {
            await requestToPromise(
              metaStore.put(
                {
                  ...state,
                  localSeq: { ...state.localSeq, [conversation]: nextSeq },
                  savedAt: new Date().toISOString(),
                },
                syncKey(user),
              ),
            );
          }
        }

        for (const message of confirmed) {
          if (message.client_message_id === "") {
            continue;
          }
          await requestToPromise(
            outboxStore.delete([user, conversation, message.client_message_id]),
          );
        }
      },
    );
  });
}

// Drops the local tail from fromSeq on (rollback resync) and lowers the local
// watermark so the next pull starts at the server's position.
export async function deleteMessagesFrom(
  user: string,
  conversation: string,
  fromSeq: number,
): Promise<void> {
  if (!Number.isInteger(fromSeq) || fromSeq < 1) {
    return;
  }
  await runWrite(async (db) => {
    await runTransaction(
      db,
      [MESSAGES_STORE, META_STORE],
      "readwrite",
      async (transaction) => {
        const store = transaction.objectStore(MESSAGES_STORE);
        const index = store.index(MESSAGES_SEQ_INDEX);
        const keys = await requestToPromise(
          index.getAllKeys(conversationSeqRange(user, conversation, fromSeq)),
        );
        for (const key of keys) {
          await requestToPromise(store.delete(key));
        }

        const metaStore = transaction.objectStore(META_STORE);
        const state = readSyncStateValue(
          await readValue(metaStore, syncKey(user)),
        );
        if (state === null) {
          return;
        }
        const keepSeq = fromSeq - 1;
        const current = state.localSeq[conversation] ?? 0;
        if (current <= keepSeq) {
          return;
        }
        const localSeq = { ...state.localSeq };
        if (keepSeq > 0) {
          localSeq[conversation] = keepSeq;
        } else {
          delete localSeq[conversation];
        }
        await requestToPromise(
          metaStore.put(
            { ...state, localSeq, savedAt: new Date().toISOString() },
            syncKey(user),
          ),
        );
      },
    );
  });
}

// The outbox keeps locally pending or failed messages across reloads so a
// message lost with the server's dropped tail can be retried with its original
// client id.
export async function writeOutbox(entry: CachedOutbox): Promise<void> {
  if (!isValidOutboxRecord(entry)) {
    return;
  }
  await runWrite(async (db) => {
    await runTransaction(db, [OUTBOX_STORE], "readwrite", async (transaction) => {
      await requestToPromise(transaction.objectStore(OUTBOX_STORE).put(entry));
    });
  });
}

export async function readOutbox(
  user: string,
  conversation: string,
): Promise<CachedOutbox[]> {
  return runRead<CachedOutbox[]>(
    [],
    async (db) =>
      runTransaction(db, [OUTBOX_STORE], "readonly", async (transaction) => {
        const records = await readAll<CachedOutbox>(
          transaction.objectStore(OUTBOX_STORE),
          conversationOutboxRange(user, conversation),
        );
        return records
          .filter(isValidOutboxRecord)
          .filter((record) =>
            record.user === user && record.conversation === conversation)
          .sort((left, right) =>
            left.created_at.localeCompare(right.created_at));
      }),
  );
}

export async function readOutboxEntry(
  user: string,
  conversation: string,
  clientMessageID: string,
): Promise<CachedOutbox | null> {
  return runRead<CachedOutbox | null>(
    null,
    async (db) =>
      runTransaction(db, [OUTBOX_STORE], "readonly", async (transaction) => {
        const value = await readValue(
          transaction.objectStore(OUTBOX_STORE),
          [user, conversation, clientMessageID],
        );
        return isValidOutboxRecord(value) ? value : null;
      }),
  );
}

export async function deleteOutbox(
  user: string,
  conversation: string,
  clientMessageID: string,
): Promise<void> {
  await runWrite(async (db) => {
    await runTransaction(db, [OUTBOX_STORE], "readwrite", async (transaction) => {
      await requestToPromise(
        transaction
          .objectStore(OUTBOX_STORE)
          .delete([user, conversation, clientMessageID]),
      );
    });
  });
}

export async function markOutboxFailed(
  user: string,
  conversation: string,
  clientMessageID: string,
): Promise<void> {
  await runWrite(async (db) => {
    await runTransaction(db, [OUTBOX_STORE], "readwrite", async (transaction) => {
      const store = transaction.objectStore(OUTBOX_STORE);
      const value = await readValue(store, [user, conversation, clientMessageID]);
      if (!isValidOutboxRecord(value)) {
        return;
      }
      await requestToPromise(store.put({ ...value, status: "failed" }));
    });
  });
}


export async function readSyncState(user: string): Promise<SyncState> {
  return runRead<SyncState>(
    emptySyncState(),
    async (db) =>
      runTransaction(db, [META_STORE], "readonly", async (transaction) => {
        const value = await readValue(
          transaction.objectStore(META_STORE),
          syncKey(user),
        );
        if (value === undefined) {
          return emptySyncState();
        }
        // A corrupt record must not silently restore trust in the cache.
        return readSyncStateValue(value) ?? {
          ...emptySyncState(),
          directComplete: false,
        };
      }),
  );
}

export async function writeSyncState(
  user: string,
  state: SyncState,
): Promise<void> {
  await runWrite(async (db) => {
    await runTransaction(db, [META_STORE], "readwrite", async (transaction) => {
      const store = transaction.objectStore(META_STORE);
      const current = readSyncStateValue(await readValue(store, syncKey(user)))
        ?? emptySyncState();
      const next: SyncState = {
        afterID: Math.max(
          current.afterID,
          normalizeCursor(state.afterID, current.afterID),
        ),
        directComplete: state.directComplete,
        groups: mergeGroupCursors(current.groups, state.groups),
        localSeq: mergeLocalSeqs(current.localSeq, state.localSeq),
        savedAt: state.savedAt === "" ? new Date().toISOString() : state.savedAt,
      };
      await requestToPromise(store.put(next, syncKey(user)));
    });
  });
}

export async function patchSyncState(
  user: string,
  patch: Partial<SyncState>,
): Promise<void> {
  await runWrite(async (db) => {
    await updateSyncState(db, user, (current) => ({
      afterID: Math.max(
        current.afterID,
        normalizeCursor(patch.afterID, current.afterID),
      ),
      directComplete: patch.directComplete ?? current.directComplete,
      groups: patch.groups !== undefined
        ? mergeGroupCursors(current.groups, patch.groups)
        : current.groups,
      localSeq: patch.localSeq !== undefined
        ? mergeLocalSeqs(current.localSeq, patch.localSeq)
        : current.localSeq,
      savedAt: new Date().toISOString(),
    }));
  });
}

export async function setGroupCursor(
  user: string,
  groupID: number,
  cursor: number,
): Promise<void> {
  if (!Number.isInteger(groupID) || groupID <= 0) {
    return;
  }
  if (!Number.isInteger(cursor) || cursor <= 0) {
    return;
  }
  await runWrite(async (db) => {
    await updateSyncState(db, user, (current) => ({
      ...current,
      groups: {
        ...current.groups,
        [groupID]: Math.max(current.groups[groupID] ?? 0, cursor),
      },
      savedAt: new Date().toISOString(),
    }));
  });
}

export async function removeGroupCursor(
  user: string,
  groupID: number,
): Promise<void> {
  await runWrite(async (db) => {
    await updateSyncState(db, user, (current) => {
      if (!(groupID in current.groups)) {
        return current;
      }
      const groups = { ...current.groups };
      delete groups[groupID];
      return { ...current, groups, savedAt: new Date().toISOString() };
    });
  });
}

export async function removeMessages(
  user: string,
  conversation: string,
): Promise<void> {
  await runWrite(async (db) => {
    await runTransaction(
      db,
      [MESSAGES_STORE, META_STORE],
      "readwrite",
      async (transaction) => {
        await requestToPromise(
          transaction
            .objectStore(MESSAGES_STORE)
            .delete(conversationMessagesRange(user, conversation)),
        );

        const metaStore = transaction.objectStore(META_STORE);
        const state = readSyncStateValue(
          await readValue(metaStore, syncKey(user)),
        );
        if (state === null || !(conversation in state.localSeq)) {
          return;
        }
        const localSeq = { ...state.localSeq };
        delete localSeq[conversation];
        await requestToPromise(
          metaStore.put(
            { ...state, localSeq, savedAt: new Date().toISOString() },
            syncKey(user),
          ),
        );
      },
    );
  });
}

export async function removeConversation(
  user: string,
  key: string,
): Promise<void> {  await runWrite(async (db) => {
    await runTransaction(
      db,
      [CONVERSATIONS_STORE, MESSAGES_STORE, META_STORE, OUTBOX_STORE],
      "readwrite",
      async (transaction) => {
        await requestToPromise(
          transaction.objectStore(CONVERSATIONS_STORE).delete([user, key]),
        );
        await requestToPromise(
          transaction
            .objectStore(MESSAGES_STORE)
            .delete(conversationMessagesRange(user, key)),
        );
        await requestToPromise(
          transaction
            .objectStore(OUTBOX_STORE)
            .delete(conversationOutboxRange(user, key)),
        );

        const metaStore = transaction.objectStore(META_STORE);
        const state = readSyncStateValue(
          await readValue(metaStore, syncKey(user)),
        );
        if (state === null) {
          return;
        }
        const groupID = groupIDFromKey(key);
        const hasCursor = groupID !== null && groupID in state.groups;
        if (!hasCursor && !(key in state.localSeq)) {
          return;
        }
        const groups = { ...state.groups };
        if (groupID !== null) {
          delete groups[groupID];
        }
        const localSeq = { ...state.localSeq };
        delete localSeq[key];
        await requestToPromise(
          metaStore.put(
            { ...state, groups, localSeq, savedAt: new Date().toISOString() },
            syncKey(user),
          ),
        );
      },
    );
  });
}

export async function clearUser(user: string): Promise<void> {
  // Drop the offline hint first: even if clearing messages fails, the next
  // person at this machine must not be shown the signed-out account's cache.
  await runWrite(async (db) => {
    await runTransaction(db, [META_STORE], "readwrite", async (transaction) => {
      const metaStore = transaction.objectStore(META_STORE);
      const activeUser = readUserValue(
        await readValue(metaStore, ACTIVE_USER_KEY),
      );
      if (activeUser?.username === user) {
        await requestToPromise(metaStore.delete(ACTIVE_USER_KEY));
      }
    });
  });

  await runWrite(async (db) => {
    await runTransaction(
      db,
      [CONVERSATIONS_STORE, MESSAGES_STORE, FRIENDS_STORE, META_STORE, OUTBOX_STORE],
      "readwrite",
      async (transaction) => {
        const conversationStore = transaction.objectStore(CONVERSATIONS_STORE);
        const messageStore = transaction.objectStore(MESSAGES_STORE);
        const friendsStore = transaction.objectStore(FRIENDS_STORE);
        const metaStore = transaction.objectStore(META_STORE);
        const outboxStore = transaction.objectStore(OUTBOX_STORE);

        const conversationKeys = await requestToPromise(
          conversationStore.getAllKeys(),
        );
        for (const key of conversationKeys) {
          if (Array.isArray(key) && key[0] === user) {
            await requestToPromise(conversationStore.delete(key));
          }
        }

        const messageKeys = await requestToPromise(messageStore.getAllKeys());
        for (const key of messageKeys) {
          if (Array.isArray(key) && key[0] === user) {
            await requestToPromise(messageStore.delete(key));
          }
        }

        const outboxKeys = await requestToPromise(outboxStore.getAllKeys());
        for (const key of outboxKeys) {
          if (Array.isArray(key) && key[0] === user) {
            await requestToPromise(outboxStore.delete(key));
          }
        }

        await requestToPromise(friendsStore.delete([user]));
        await requestToPromise(metaStore.delete(syncKey(user)));
        // v1.6 wrote its pending read positions here; drop the leftover key.
        await requestToPromise(metaStore.delete(`${user}:pending-read`));
      },
    );
  });
}

async function dropGroupCursors(
  metaStore: IDBObjectStore,
  user: string,
  groupIDs: number[],
): Promise<void> {
  if (groupIDs.length === 0) {
    return;
  }
  const state = readSyncStateValue(await readValue(metaStore, syncKey(user)));
  if (state === null) {
    return;
  }
  let changed = false;
  for (const groupID of groupIDs) {
    if (groupID in state.groups) {
      delete state.groups[groupID];
      changed = true;
    }
  }
  if (changed) {
    await requestToPromise(metaStore.put(state, syncKey(user)));
  }
}

async function dropLocalSeqs(
  metaStore: IDBObjectStore,
  user: string,
  keys: string[],
): Promise<void> {
  if (keys.length === 0) {
    return;
  }
  const state = readSyncStateValue(await readValue(metaStore, syncKey(user)));
  if (state === null) {
    return;
  }
  let changed = false;
  for (const key of keys) {
    if (key in state.localSeq) {
      delete state.localSeq[key];
      changed = true;
    }
  }
  if (changed) {
    await requestToPromise(metaStore.put(state, syncKey(user)));
  }
}

export async function enforceUserLimits(
  user: string,
  options: { maxMessages?: number; maxConversations?: number } = {},
): Promise<void> {
  const maxMessages = normalizeLimit(options.maxMessages, MESSAGE_LIMIT_PER_USER);
  const maxConversations = normalizeLimit(
    options.maxConversations,
    CONVERSATION_CACHE_LIMIT,
  );

  await runWrite(async (db) => {
    await runTransaction(
      db,
      [MESSAGES_STORE, META_STORE],
      "readwrite",
      async (transaction) => {
        const messageStore = transaction.objectStore(MESSAGES_STORE);
        const metaStore = transaction.objectStore(META_STORE);

        const messageRecords = await readAll<CachedMessageRecord>(
          messageStore,
          userMessagesRange(user),
        );
        const stats = new Map<string, { count: number; maxID: number }>();
        for (const record of messageRecords) {
          if (!isValidMessageRecord(record) || record.user !== user) {
            continue;
          }
          const entry = stats.get(record.conversation) ?? { count: 0, maxID: 0 };
          entry.count += 1;
          if (record.id > entry.maxID) {
            entry.maxID = record.id;
          }
          stats.set(record.conversation, entry);
        }

        let conversationCount = stats.size;
        let messageCount = 0;
        for (const entry of stats.values()) {
          messageCount += entry.count;
        }
        if (
          conversationCount <= maxConversations &&
          messageCount <= maxMessages
        ) {
          return;
        }

        // Evict by the newest global message id: it is the only ordering that
        // is comparable across conversations. seq is conversation-local and
        // must never be used here.
        const candidates = Array.from(stats.keys());
        candidates.sort((left, right) => {
          const leftID = stats.get(left)?.maxID ?? 0;
          const rightID = stats.get(right)?.maxID ?? 0;
          if (leftID !== rightID) {
            return leftID - rightID;
          }
          return left.localeCompare(right);
        });

        const removedGroups: number[] = [];
        const removedKeys: string[] = [];
        for (const key of candidates) {
          if (
            conversationCount <= maxConversations &&
            messageCount <= maxMessages
          ) {
            break;
          }
          const entry = stats.get(key) ?? { count: 0, maxID: 0 };
          await requestToPromise(
            messageStore.delete(conversationMessagesRange(user, key)),
          );
          conversationCount -= 1;
          messageCount -= entry.count;
          removedKeys.push(key);
          const groupID = groupIDFromKey(key);
          if (groupID !== null) {
            removedGroups.push(groupID);
          }
        }

        await dropGroupCursors(metaStore, user, removedGroups);
        await dropLocalSeqs(metaStore, user, removedKeys);
      },
    );
  });
}
