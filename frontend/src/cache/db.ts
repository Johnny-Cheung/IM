const DB_NAME = "im-cache";
const DB_VERSION = 1;

export const META_STORE = "meta";
export const CONVERSATIONS_STORE = "conversations";
export const MESSAGES_STORE = "messages";
export const FRIENDS_STORE = "friends";

export type CacheStoreName =
  | typeof META_STORE
  | typeof CONVERSATIONS_STORE
  | typeof MESSAGES_STORE
  | typeof FRIENDS_STORE;

type StoreDefinition = {
  name: CacheStoreName;
  keyPath: string | string[] | null;
};

const STORE_DEFINITIONS: StoreDefinition[] = [
  { name: META_STORE, keyPath: null },
  { name: CONVERSATIONS_STORE, keyPath: ["user", "key"] },
  { name: MESSAGES_STORE, keyPath: ["user", "conversation", "id"] },
  { name: FRIENDS_STORE, keyPath: ["user"] },
];

let dbPromise: Promise<IDBDatabase> | null = null;
let openDB: IDBDatabase | null = null;

function createStores(db: IDBDatabase): void {
  for (const name of Array.from(db.objectStoreNames)) {
    db.deleteObjectStore(name);
  }
  for (const definition of STORE_DEFINITIONS) {
    if (definition.keyPath === null) {
      db.createObjectStore(definition.name);
    } else {
      db.createObjectStore(definition.name, { keyPath: definition.keyPath });
    }
  }
}

export function openCacheDB(): Promise<IDBDatabase> {
  if (dbPromise !== null) {
    return dbPromise;
  }
  if (typeof indexedDB === "undefined") {
    return Promise.reject(new Error("IndexedDB is not available"));
  }

  const opening = new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);

    request.onupgradeneeded = () => {
      createStores(request.result);
    };

    request.onsuccess = () => {
      const db = request.result;
      openDB = db;
      db.onversionchange = () => {
        db.close();
        if (openDB === db) {
          openDB = null;
        }
        dbPromise = null;
      };
      db.onclose = () => {
        if (openDB === db) {
          openDB = null;
        }
        dbPromise = null;
      };
      resolve(db);
    };

    request.onerror = () => {
      dbPromise = null;
      reject(request.error ?? new Error("Failed to open the cache database"));
    };
  });

  dbPromise = opening.catch((error: unknown) => {
    dbPromise = null;
    throw error;
  });
  return dbPromise;
}

export async function closeCacheDB(): Promise<void> {
  const pending = dbPromise;
  dbPromise = null;
  openDB = null;
  if (pending === null) {
    return;
  }
  try {
    const db = await pending;
    db.close();
    if (openDB === db) {
      openDB = null;
    }
  } catch {
    // The database never opened; nothing to close.
  }
}

export async function deleteCacheDB(): Promise<void> {
  await closeCacheDB();
  if (typeof indexedDB === "undefined") {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase(DB_NAME);
    request.onsuccess = () => resolve();
    request.onerror = () =>
      reject(request.error ?? new Error("Failed to delete the cache database"));
    request.onblocked = () =>
      reject(new Error("The cache database deletion is blocked"));
  });
}

export function requestToPromise<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () =>
      reject(request.error ?? new Error("IndexedDB request failed"));
  });
}

export function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () =>
      reject(transaction.error ?? new Error("IndexedDB transaction failed"));
    transaction.onabort = () =>
      reject(transaction.error ?? new Error("IndexedDB transaction aborted"));
  });
}
