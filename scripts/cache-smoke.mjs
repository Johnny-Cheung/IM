#!/usr/bin/env node
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { Browser } from "./lib/browser.mjs";
import { delay, TestRun } from "./lib/test-client.mjs";

const run = new TestRun("cache-smoke");
const browser = new Browser();
let tab;
let failures = 0;
const name = (key) => run.users.get(key).username;
const directSelector = (key) => `.conversation-item[data-username="${name(key)}"]`;
const directOf = (conversationList, username) => conversationList.conversations.find((item) => item.type === "direct" && item.user.username === username);
const conversationsOf = (who) => run.request(who, "GET", "/api/conversations");
// a's unread from c: the browser opens b, so only c's message stays unread.
// The browser owns a's session, so ask through the page instead of HTTP.
async function unreadFromC() {
  return tab.evaluate(`(async () => {
    const response = await fetch('/api/conversations', { credentials: 'include' });
    const data = await response.json();
    const item = data.conversations.find((conversation) =>
      conversation.type === "direct" &&
      conversation.user.username.toLowerCase() === ${JSON.stringify(name("c"))}.toLowerCase());
    return item?.unread_count ?? 0;
  })()`);
}

// Offline variant: read the locally cached sidebar value, since no request can
// reach the server while the network is disabled.
async function cachedUnreadFromC() {
  return tab.evaluate(`(async () => {
    const db = await new Promise((resolve, reject) => {
      const request = indexedDB.open("im-cache");
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    const rows = await new Promise((resolve) => {
      const transaction = db.transaction(["conversations"], "readonly");
      const request = transaction.objectStore("conversations").getAll();
      transaction.oncomplete = () => resolve(request.result ?? []);
    });
    db.close();
    const row = rows.find((entry) =>
      entry.user === ${JSON.stringify(name("a"))} &&
      entry.conversation?.type === "direct" &&
      entry.conversation.user.username.toLowerCase() === ${JSON.stringify(name("c"))}.toLowerCase());
    return row?.conversation?.unread_count ?? 0;
  })()`);
}
const same = (first, second) => String(first).toLocaleLowerCase() === String(second).toLocaleLowerCase();

function trackReadRequests() {
  const paths = [];
  const off = tab.on("Network.requestWillBeSent", ({ request }) => {
    try {
      const url = new URL(request.url);
      if (url.origin === run.base && (url.pathname === "/api/read" || url.pathname.endsWith("/read"))) {
        paths.push(url.pathname);
      }
    } catch {
      // Ignore non-URL requests.
    }
  });
  return { paths, off };
}

function trackFocusFrames() {
  const frames = [];
  const off = tab.on("Network.webSocketFrameSent", ({ response }) => {
    try {
      const frame = JSON.parse(response.payloadData);
      if (frame?.type === "focus" || frame?.type === "blur") frames.push(frame);
    } catch {
      // Ignore non-JSON frames.
    }
  });
  return { frames, off };
}

async function waitUntil(condition, message, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await delay(100);
  }
  throw new Error(message);
}

async function scenario(title, action) {
  try {
    await action();
    console.log(`SCENARIO PASS ${title}`);
  } catch (error) {
    failures += 1;
    console.error(`SCENARIO FAIL ${title}: ${error.message}`);
    await tab.shot(join(run.reportDir, `failed-${failures}.png`)).catch(() => undefined);
  }
}

async function setOffline(offline) {
  await tab.send("Network.emulateNetworkConditions", {
    offline,
    latency: 0,
    downloadThroughput: -1,
    uploadThroughput: -1,
  });
}

async function cacheState(user) {
  return tab.evaluate(`(async () => {
    const db = await new Promise((resolve, reject) => {
      const request = indexedDB.open("im-cache");
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    if (!db.objectStoreNames.contains("meta") || !db.objectStoreNames.contains("messages")) {
      const version = db.version;
      db.close();
      return { version, active: null, messages: 0, withoutSeq: [] };
    }
    const version = db.version;
    const result = await new Promise((resolve) => {
      const transaction = db.transaction(["meta", "messages"], "readonly");
      const active = transaction.objectStore("meta").get("active-user");
      const rows = transaction.objectStore("messages").getAll();
      transaction.oncomplete = () => {
        const cached = (rows.result ?? []).filter((row) => row && row.user === ${JSON.stringify(user)});
        resolve({
          active: active.result ?? null,
          messages: cached.length,
          withoutSeq: cached
            .filter((row) => !Number.isInteger(row.seq) || row.seq < 0)
            .map((row) => row.id),
        });
      };
    });
    db.close();
    return { version, ...result };
  })()`);
}

async function metaKeys() {
  return tab.evaluate(`(async () => {
    const known = await indexedDB.databases();
    if (!known.some((database) => database.name === "im-cache")) {
      return [];
    }
    const db = await new Promise((resolve, reject) => {
      const request = indexedDB.open("im-cache");
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    if (!db.objectStoreNames.contains("meta")) {
      db.close();
      return [];
    }
    const keys = await new Promise((resolve) => {
      const transaction = db.transaction(["meta"], "readonly");
      const request = transaction.objectStore("meta").getAllKeys();
      transaction.oncomplete = () => resolve(request.result ?? []);
    });
    db.close();
    return keys.map(String);
  })()`);
}

async function main() {
  console.log(`Cache smoke ${run.id}: ${run.base}`);
  for (const key of ["a", "b", "c"]) await run.register(key);
  await run.befriend("b", "a");
  await run.befriend("a", "c");

  await browser.start();
  tab = await browser.tab(run.base);
  // Seed the old cache from a same-origin document that does not run the app,
  // so the app cannot upgrade the database before the fixture exists.
  await tab.send("Page.navigate", { url: `${run.base}/health` });
  await tab.waitFor(`location.origin === ${JSON.stringify(run.base)} && document.readyState === 'complete'`);
  const legacyBody = `旧版无编号缓存 ${run.id}`;
  await tab.evaluate(`(async () => {
    await new Promise((resolve, reject) => {
      const request = indexedDB.open("im-cache", 1);
      request.onupgradeneeded = () => {
        const db = request.result;
        db.createObjectStore("meta");
        db.createObjectStore("conversations", { keyPath: ["user", "key"] });
        db.createObjectStore("messages", { keyPath: ["user", "conversation", "id"] });
        db.createObjectStore("friends", { keyPath: ["user"] });
      };
      request.onsuccess = () => {
        const db = request.result;
        const transaction = db.transaction(["meta", "messages"], "readwrite");
        transaction.objectStore("meta").put(
          { afterID: 900, directComplete: true, groups: {}, localSeq: {} },
          ${JSON.stringify(name("a"))} + ":sync",
        );
        transaction.objectStore("messages").put({
          user: ${JSON.stringify(name("a"))},
          conversation: "direct:" + ${JSON.stringify(name("b"))},
          id: 900,
          body: ${JSON.stringify(legacyBody)},
          sender: { username: ${JSON.stringify(name("b"))}, nickname: "旧" },
          recipient: { username: ${JSON.stringify(name("a"))}, nickname: "旧" },
          client_message_id: "legacy-900",
          created_at: "2026-01-01T00:00:00Z",
        });
        transaction.oncomplete = () => { db.close(); resolve(true); };
        transaction.onerror = () => reject(transaction.error);
        transaction.onabort = () => reject(transaction.error);
      };
      request.onerror = () => reject(request.error);
    });
    return true;
  })()`);
  await tab.login(name("a"), run.password);
  await tab.waitFor("!!document.querySelector('#conversation-list .conversation-item')");
  await delay(300);
  const loginKeys = await metaKeys();
  run.check("fresh login has no pending read queue", !loginKeys.includes(`${name("a")}:pending-read`));

  const fromB = `缓存测试 B→A ${run.id}`;
  const fromC = `仅 A 与 C ${run.id}`;
  await run.request("b", "POST", `/api/conversations/${name("a")}/messages`, {
    body: fromB,
    client_message_id: randomUUID(),
  });
  await run.request("c", "POST", `/api/conversations/${name("a")}/messages`, {
    body: fromC,
    client_message_id: randomUUID(),
  });
  await tab.waitFor(`!!document.querySelector('#conversation-list ${directSelector("b")}')`);
  await tab.waitFor(`!!document.querySelector('#conversation-list ${directSelector("c")}')`);

  await scenario("a v1.7 cache is discarded on the version upgrade", async () => {
    const state = await cacheState(name("a"));
    console.log(`Cache after upgrade: ${JSON.stringify(state)}`);
    run.check("cache version is upgraded", state.version === 2);
    run.check("pre-seq cache rows are gone", state.withoutSeq.length === 0);
    await tab.select(directSelector("b"), `/chat/${name("b")}`);
    await tab.waitFor(`document.querySelector('#message-list')?.innerText.includes(${JSON.stringify(fromB)})`);
    run.check(
      "legacy cached body is not rendered",
      !await tab.evaluate(`document.body.innerText.includes(${JSON.stringify(legacyBody)})`),
    );
  });

  await scenario("cold reload renders the cached conversation with one bootstrap", async () => {
    await tab.select(directSelector("b"), `/chat/${name("b")}`);
    await tab.waitFor(`document.querySelector('#message-list').innerText.includes(${JSON.stringify(fromB)})`);

    const requests = [];
    const off = tab.on("Network.requestWillBeSent", ({ request }) => {
      try {
        const url = new URL(request.url);
        if (url.origin === run.base && url.pathname.startsWith("/api/")) {
          requests.push(url.pathname);
        }
      } catch {
        // Ignore non-URL requests.
      }
    });
    try {
      await tab.send("Page.reload");
      await tab.waitFor("document.readyState === 'complete'");
      await tab.waitFor("!!document.querySelector('#conversation-list .conversation-item')");
      await tab.waitFor(`document.querySelector('#message-list')?.innerText.includes(${JSON.stringify(fromB)})`, 5000);
      await delay(800);
    } finally {
      off();
    }
    console.log(`Cold start API requests: ${JSON.stringify(requests)}`);
    run.check("cold start sends one bootstrap request", requests.filter((path) => path === "/api/bootstrap").length === 1);
    run.check("bootstrap is the first-screen request", requests[0] === "/api/bootstrap");
    run.check("cold start does not refetch friends", !requests.includes("/api/friends"));
    run.check(
      "cold start sends no read request",
      !requests.some((path) => path === "/api/read" || path.endsWith("/read")),
    );
    run.check(
      "cold start does not refetch conversations",
      !requests.includes("/api/conversations"),
    );
    run.check(
      "cold start does not refetch the open conversation",
      !requests.some((path) => path === `/api/conversations/${name("b")}/messages`),
    );
    const state = await cacheState(name("a"));
    run.check("cached messages carry conversation seqs", state.withoutSeq.length === 0 && state.messages > 0);
  });

  await scenario("offline mode stays readable and disables sending", async () => {
    const reads = trackReadRequests();
    try {
      run.check("cached conversation starts unread", await unreadFromC() === 1);
      await setOffline(true);
      await tab.waitFor("!!document.querySelector('#offline-banner')", 5000);
      run.check(
        "cached messages stay visible offline",
        await tab.evaluate(`document.querySelector('#message-list').innerText.includes(${JSON.stringify(fromB)})`),
      );
      run.check(
        "composer is disabled offline",
        await tab.evaluate("document.querySelector('#message-input').disabled"),
      );
      await tab.evaluate(`document.querySelector('#conversation-list ${directSelector("c")}').click()`);
      await tab.waitFor(`location.pathname === "/chat/${name("c")}"`);
      await tab.waitFor(`document.querySelector('#message-list').innerText.includes(${JSON.stringify(fromC)})`);
      run.check("another cached conversation opens offline", true);
      run.check(
        "offline reading leaves the cached unread untouched",
        await cachedUnreadFromC() === 1,
      );
      run.check("offline reading sends no read request", reads.paths.length === 0);
    } finally {
      reads.off();
    }
  });

  await scenario("reconnecting restores the live connection and refocuses", async () => {
    const reads = trackReadRequests();
    const focus = trackFocusFrames();
    try {
      await setOffline(false);
      await tab.evaluate("window.dispatchEvent(new Event('online'))");
      await tab.waitFor("!document.querySelector('#offline-banner')", 5000);
      await tab.waitFor("!!document.querySelector('.connection-state--open')", 10000);
      run.check("composer becomes editable again", !await tab.evaluate("document.querySelector('#message-input').disabled"));

      await waitUntil(
        () => focus.frames.some((frame) => frame.type === "focus" && frame.kind === "direct" && same(frame.username, name("c"))),
        "reconnect did not refocus the cached conversation",
      );
      await waitUntil(
        async () => await unreadFromC() === 0,
        "reconnect did not clear the unread on the server",
      );
      run.check("reconnect clears the offline read on the server", true);
      run.check("reconnect sends no read request", reads.paths.length === 0);
      const keys = await metaKeys();
      run.check("reconnect leaves no pending read queue", !keys.includes(`${name("a")}:pending-read`));
    } finally {
      reads.off();
      focus.off();
    }
  });

  await scenario("logout clears this account's cache", async () => {
    await tab.evaluate(
      "[...document.querySelectorAll('.chat-nav button')].find((button) => button.textContent.includes('退出'))?.click()",
    );
    await tab.waitFor("location.pathname === '/login'", 5000);
    await delay(300);
    const state = await cacheState(name("a"));
    console.log(`Cache after logout: ${JSON.stringify(state)}`);
    run.check("logout clears the active user hint", state.active === null);
    run.check("logout clears cached messages", state.messages === 0);
  });

  await scenario("another account cannot read the previous cache", async () => {
    await tab.login(name("b"), run.password);
    await tab.waitFor("!!document.querySelector('#conversation-list .conversation-item')");
    await delay(400);
    run.check(
      "previous account's private message is not shown",
      !await tab.evaluate(`document.body.innerText.includes(${JSON.stringify(fromC)})`),
    );
    run.check(
      "previous account's conversation is not shown",
      !await tab.evaluate(`!!document.querySelector('#conversation-list ${directSelector("c")}')`),
    );
  });

  run.check("no browser runtime exception", tab.exceptions.length === 0);
  if (failures) throw new Error(`${failures} cache scenarios failed`);
}

try {
  await main();
} catch (error) {
  console.error(`FAIL ${error.message}`);
  process.exitCode = 1;
} finally {
  await browser.close();
  await run.finish();
}
