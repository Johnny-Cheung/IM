#!/usr/bin/env node
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { Browser } from "./lib/browser.mjs";
import { delay, TestRun } from "./lib/test-client.mjs";

const run = new TestRun("browser-smoke");
const browser = new Browser();
let tab;
let failures = 0;
const name = (key) => run.users.get(key).username;
const memberSelector = (id) => `.conversation-item[data-group-id="${id}"]`;
const directSelector = (key) => `.conversation-item[data-username="${name(key)}"]`;
const textAt = (selector) => `document.querySelector(${JSON.stringify(selector)})?.textContent`;
const conversationsOf = (who) => run.request(who, "GET", "/api/conversations");
const directOf = (list, name) => list.conversations.find((item) => item.type === "direct" && item.user.username === name);
const browserConversations = () => tab.evaluate("fetch('/api/conversations', { credentials: 'same-origin' }).then((response) => response.json())");
const groupOf = (conversationList, id) => conversationList.conversations.find((item) => item.type === "group" && item.group.id === id);
const same = (first, second) => String(first).toLocaleLowerCase() === String(second).toLocaleLowerCase();
const focusTarget = (frames, kind, value) => frames.some((frame) =>
  frame.type === "focus" && frame.kind === kind
    && (kind === "direct" ? same(frame.username, value) : frame.group_id === value));

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

async function hidePage() {
  await tab.evaluate("Object.defineProperty(document, 'visibilityState', {configurable:true, get:()=>window.__imHidden ? 'hidden' : 'visible'}); window.__imHidden=true; document.dispatchEvent(new Event('visibilitychange'))");
}

async function showPage() {
  await tab.evaluate("window.__imHidden=false; document.dispatchEvent(new Event('visibilitychange'))");
}

// Reads the receipt rendered under the own message whose body is given.
const receiptOf = (body) => `(() => {
  const row = [...document.querySelectorAll('#message-list .message-row')]
    .find((el) => el.innerText.includes(${JSON.stringify(body)}));
  const state = row?.querySelector('.message-meta > span.read-state');
  return state ? { text: state.textContent, read: state.dataset.readCount, members: state.dataset.memberCount, all: state.classList.contains('read-state--read') } : null;
})()`;

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

async function openGroup(id) {
  await tab.evaluate("document.querySelector('#group-info-panel .modal-header button')?.click()");
  await tab.select(memberSelector(id), `/chat/group/${id}`);
  await tab.waitFor("!!document.querySelector('.connection-state--open')");
  await tab.click("#group-info-button");
  await tab.waitFor("!!document.querySelector('#group-info-panel') && !!document.querySelector('#group-info-name-input')");
}

async function offline() {
  await tab.send("Network.emulateNetworkConditions", { offline: true, latency: 0, downloadThroughput: -1, uploadThroughput: -1 });
  // Close the real app connection after preventing its automatic retry.
  await tab.evaluate("window.__imTestSockets.forEach(socket => socket.close())");
  await tab.waitFor("!document.querySelector('.connection-state--open')");
}

async function online() {
  await tab.send("Network.emulateNetworkConditions", { offline: false, latency: 0, downloadThroughput: -1, uploadThroughput: -1 });
  await tab.evaluate("window.dispatchEvent(new Event('online'))");
}

async function main() {
  console.log(`Browser smoke ${run.id}: ${run.base}`);
  for (const key of ["a", "b", "c"]) await run.register(key);
  await run.befriend("b", "a");
  await run.befriend("b", "c");
  await run.befriend("a", "c");
  const renamed = await run.group("b", "断线改名前", ["a", "c"]);
  const kicked = await run.group("b", "断线移除", ["a", "c"]);
  const dissolved = await run.group("b", "断线解散", ["a", "c"]);
  const live = await run.group("b", "实时消息与未读", ["a", "c"]);
  await browser.start();
  tab = await browser.tab(run.base);
  await tab.send("Page.addScriptToEvaluateOnNewDocument", { source: `
    window.__imTestSockets = [];
    window.__imDropInbound = null;
    window.WebSocket = class extends WebSocket {
      constructor(...args) { super(...args); window.__imTestSockets.push(this); }
      set onmessage(handler) {
        super.onmessage = (event) => {
          try {
            if (window.__imDropInbound && window.__imDropInbound(event)) return;
          } catch {
            // Never let the test hook break the app's event handling.
          }
          handler.call(this, event);
        };
      }
      get onmessage() { return super.onmessage; }
    };
  ` });
  await tab.login(name("a"), run.password);
  await tab.waitFor(`!!document.querySelector('#conversation-list ${memberSelector(live)}')`);

  await scenario("switching conversations preserves sidebar and live connection", async () => {
    await tab.waitFor("window.__imTestSockets.some(socket => socket.readyState === WebSocket.OPEN)");
    await delay(350);
    let listRequests = 0;
    let newSockets = 0;
    const offRequests = tab.on("Network.requestWillBeSent", ({ request }) => {
      if (new URL(request.url).pathname === "/api/conversations") listRequests += 1;
    });
    const offSockets = tab.on("Network.webSocketCreated", () => { newSockets += 1; });
    await tab.evaluate(`(() => {
      const list = document.querySelector('#conversation-list');
      window.__imSidebar = { list, first: list.querySelector('.conversation-item'), style: list.getAttribute('style'), flashes: 0 };
      list.style.flex = '0 0 80px';
      list.style.maxHeight = '80px';
      list.scrollTop = 90;
      window.__imSidebar.top = list.scrollTop;
      window.__imSidebar.observer = new MutationObserver(() => {
        if (list.innerText.includes('正在加载会话')) window.__imSidebar.flashes += 1;
      });
      window.__imSidebar.observer.observe(list, { childList: true, subtree: true });
    })()`);
    try {
      for (const [selector, path] of [
        [directSelector("b"), `/chat/${name("b")}`],
        [directSelector("c"), `/chat/${name("c")}`],
        [memberSelector(renamed), `/chat/group/${renamed}`],
        [memberSelector(live), `/chat/group/${live}`],
        [directSelector("b"), `/chat/${name("b")}`],
      ]) await tab.select(selector, path);
      await delay(200);
      const state = await tab.evaluate(`(() => {
        const saved = window.__imSidebar;
        const list = document.querySelector('#conversation-list');
        return { sameList: list === saved.list, sameFirst: list.querySelector('.conversation-item') === saved.first,
          flashes: saved.flashes, before: saved.top, after: list.scrollTop,
          selected: list.querySelectorAll('[aria-current="page"]').length };
      })()`);
      console.log(`Sidebar switch metrics: ${JSON.stringify({ ...state, listRequests, newSockets })}`);
      run.check("sidebar element survives route changes", state.sameList);
      run.check("sidebar rows are retained", state.sameFirst);
      run.check("sidebar never re-enters loading state", state.flashes === 0);
      run.check("sidebar scroll position is preserved", state.before > 0 && state.before === state.after);
      run.check("empty-chat switches do not refetch conversations", listRequests === 0);
      run.check("chat switches keep the existing WebSocket", newSockets === 0);
      run.check("selected conversation highlight updates", state.selected === 1);
    } finally {
      offRequests();
      offSockets();
      await tab.evaluate(`(() => {
        const saved = window.__imSidebar;
        saved.observer.disconnect();
        const list = document.querySelector('#conversation-list');
        if (saved.style === null) list.removeAttribute('style');
        else list.setAttribute('style', saved.style);
      })()`);
    }
  });

  await scenario("delayed send failure stays in originating chat", async () => {
    await tab.select(directSelector("b"), `/chat/${name("b")}`);
    let resolvePaused;
    const paused = new Promise((resolve) => { resolvePaused = resolve; });
    const off = tab.on("Fetch.requestPaused", resolvePaused);
    await tab.send("Fetch.enable", { patterns: [{ urlPattern: `*/api/conversations/${name("b")}/messages`, requestStage: "Request" }] });
    try {
      await tab.setValue("#message-input", "延迟失败");
      await tab.click("#send-message");
      const request = await Promise.race([paused, delay(5000).then(() => { throw new Error("send was not intercepted"); })]);
      await tab.select(directSelector("c"), `/chat/${name("c")}`);
      await tab.send("Fetch.fulfillRequest", {
        requestId: request.requestId,
        responseCode: 403,
        responseHeaders: [{ name: "Content-Type", value: "application/json" }],
        body: Buffer.from(JSON.stringify({ error: { code: "not_friends", message: "验证用延迟失败" } })).toString("base64"),
      });
      await delay(350);
      run.check("new chat has no old error", !await tab.evaluate("document.body.innerText.includes('验证用延迟失败')"));
      run.check("new chat remains writable", !await tab.evaluate("document.querySelector('#message-input').disabled || !!document.querySelector('#chat-readonly')"));
    } finally {
      off();
      await tab.send("Fetch.disable");
    }
  });

  await scenario("offline rename refreshes detail on reconnect", async () => {
    await openGroup(renamed);
    await offline();
    try {
      await run.request("b", "PATCH", `/api/groups/${renamed}`, { name: "恢复后新群名" });
    } finally { await online(); }
    await tab.waitFor("!!document.querySelector('.connection-state--open')");
    await tab.waitFor(`${textAt('.message-header h2')} === '恢复后新群名'`, 5000);
    await tab.waitFor("document.querySelector('#group-info-name-input')?.value === '恢复后新群名'", 5000);
    run.check("reconnect updates group name and open panel", true);
  });

  for (const [id, action] of [[kicked, "kick"], [dissolved, "dissolve"]]) {
    await scenario(`offline ${action} removes stale group`, async () => {
      await openGroup(id);
      await offline();
      try {
        if (action === "kick") await run.request("b", "DELETE", `/api/groups/${id}/members/${name("a")}`);
        else await run.request("b", "POST", `/api/groups/${id}/leave`);
      } finally { await online(); }
      await tab.waitFor("location.pathname === '/'", 5000);
      run.check(`${action} clears details and messages`, !await tab.evaluate("!!document.querySelector('#group-info-panel') || !!document.querySelector('#message-list .message-row')"));
      run.check(`${action} removes conversation`, !await tab.evaluate(`!!document.querySelector('#conversation-list ${memberSelector(id)}')`));
      run.check(`${action} explains lost access`, await tab.evaluate("document.querySelector('#chat-notice')?.textContent.includes('群聊已解散或你已不在群中')"));
    });
  }

  await scenario("real-time group unread, background, history and mobile", async () => {
    await tab.evaluate("document.querySelector('#group-info-panel .modal-header button')?.click()");
    await tab.select(directSelector("c"), `/chat/${name("c")}`);
    const firstBody = `实时推送 ${run.id}`;
    await run.request("b", "POST", `/api/groups/${live}/messages`, { body: firstBody, client_message_id: randomUUID() });
    await tab.waitFor(`document.querySelector('#conversation-list ${memberSelector(live)} .unread-badge')?.textContent === '1'`);
    run.check("group unread arrives through WebSocket", true);
    await tab.select(memberSelector(live), `/chat/group/${live}`);
    await tab.waitFor(`document.querySelector('#message-list')?.innerText.includes(${JSON.stringify(firstBody)})`);
    await tab.waitFor(`!document.querySelector('#conversation-list ${memberSelector(live)} .unread-badge')`);
    run.check("opening group clears unread", true);
    run.check("other members' messages have no read receipt", !await tab.evaluate("!!document.querySelector('#message-list .read-state')"));
    const unreadSelector = `#conversation-list ${memberSelector(live)} .unread-badge`;
    const reads = trackReadRequests();
    const focus = trackFocusFrames();
    try {
      await run.request("b", "POST", `/api/groups/${live}/messages`, { body: "前台已看到", client_message_id: randomUUID() });
      await tab.waitFor("document.querySelector('#message-list')?.innerText.includes('前台已看到')");
      await delay(600);
      run.check("foreground group message leaves no unread", !await tab.evaluate(`!!document.querySelector(${JSON.stringify(unreadSelector)})`));
      run.check("foreground viewing sends no read request", reads.paths.length === 0);

      const hiddenAt = focus.frames.length;
      await hidePage();
      await waitUntil(() => focus.frames.slice(hiddenAt).some((frame) => frame.type === "blur"), "hiding the page did not send blur");
      await delay(150);
      await run.request("b", "POST", `/api/groups/${live}/messages`, { body: "后台未读", client_message_id: randomUUID() });
      await tab.waitFor(`document.querySelector(${JSON.stringify(unreadSelector)})?.textContent === '1'`);
      await delay(400);
      run.check("hidden tab leaves the background message unread", await tab.evaluate(`document.querySelector(${JSON.stringify(unreadSelector)})?.textContent === '1'`));
      run.check("hidden tab sends no read request", reads.paths.length === 0);
      run.check("hidden unread is kept on the server", groupOf(await browserConversations(), live)?.unread_count === 1);

      const shownAt = focus.frames.length;
      await showPage();
      await tab.waitFor(`!document.querySelector(${JSON.stringify(unreadSelector)})`);
      await waitUntil(() => focusTarget(focus.frames.slice(shownAt), "group", live), "returning to foreground did not focus the group");
      await waitUntil(async () => groupOf(await browserConversations(), live)?.unread_count === 0, "server unread did not clear after refocus");
      run.check("returning to foreground catches up the unread", true);
      run.check("foreground catch-up uses focus instead of read requests", reads.paths.length === 0);
    } finally {
      await showPage();
      reads.off();
      focus.off();
    }
    for (let index = 0; index < 32; index += 1) await run.request("b", "POST", `/api/groups/${live}/messages`, { body: `历史 ${index}`, client_message_id: randomUUID() });
    // Wait until every real-time message has reached the client before
    // dropping the cache; a late WebSocket write would otherwise restore a
    // single cached row and skip the network page.
    await tab.waitFor("document.querySelectorAll('#message-list .message-row').length === 35", 5000);
    // Drop this group's local cache so the refresh still exercises the
    // network pagination path instead of serving everything from IndexedDB.
    await tab.evaluate(`(async () => {
      const db = await new Promise((resolve, reject) => {
        const request = indexedDB.open("im-cache");
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      const key = ${JSON.stringify(`group:${live}`)};
      const user = ${JSON.stringify(name("a"))};
      await new Promise((resolve) => {
        const transaction = db.transaction(["messages", "meta"], "readwrite");
        const messages = transaction.objectStore("messages");
        const keys = messages.getAllKeys();
        keys.onsuccess = () => {
          for (const item of keys.result) {
            if (Array.isArray(item) && item[0] === user && item[1] === key) messages.delete(item);
          }
        };
        const meta = transaction.objectStore("meta");
        const sync = meta.get(user + ":sync");
        sync.onsuccess = () => {
          const state = sync.result;
          if (state && state.groups) {
            delete state.groups[${live}];
            meta.put(state, user + ":sync");
          }
        };
        transaction.oncomplete = resolve;
      });
      db.close();
    })()`);
    await tab.goto(`/chat/group/${live}`);
    await tab.waitFor("document.querySelectorAll('#message-list .message-row').length === 30");
    run.check("refresh restores group and latest page", true);
    await tab.evaluate("const el=document.querySelector('#message-list'); el.scrollTop=0; el.dispatchEvent(new Event('scroll'))");
    await tab.waitFor("document.querySelectorAll('#message-list .message-row').length === 35");
    run.check("scroll loads older group messages", true);
    await tab.viewport(390, 844);
    run.check("mobile page stays within viewport", await tab.evaluate("document.documentElement.scrollWidth <= innerWidth+1"));
    run.check("mobile input stays visible", await tab.evaluate("document.querySelector('#message-input').getBoundingClientRect().bottom <= innerHeight+1"));
    await tab.click("#group-info-button");
    await tab.waitFor("!!document.querySelector('#group-info-name-input')");
    run.check("mobile group panel fits", await tab.evaluate("document.querySelector('#group-info-panel').getBoundingClientRect().width <= innerWidth+1"));
    await tab.shot(join(run.reportDir, "mobile-group.png"));
    await tab.evaluate("document.querySelector('#group-info-panel .modal-header button').click()");
    await tab.viewport(1280, 900);
  });

  await scenario("private foreground reading uses focus with receipts", async () => {
    const reads = trackReadRequests();
    const focus = trackFocusFrames();
    try {
      await tab.select(directSelector("b"), `/chat/${name("b")}`);
      await waitUntil(() => focusTarget(focus.frames, "direct", name("b")), "opening a private chat did not focus it");
      const unread = `#conversation-list ${directSelector("b")} .unread-badge`;
      await run.request("b", "POST", `/api/conversations/${name("a")}/messages`, { body: "私聊前台观看", client_message_id: randomUUID() });
      await tab.waitFor("document.querySelector('#message-list')?.innerText.includes('私聊前台观看')");
      await delay(600);
      run.check("foreground private message leaves no unread", !await tab.evaluate(`!!document.querySelector(${JSON.stringify(unread)})`));
      run.check("private foreground sends no read request", reads.paths.length === 0);

      await tab.setValue("#message-input", "回执测试");
      await tab.click("#send-message");
      await tab.waitFor(`(() => {
        const row = [...document.querySelectorAll('#message-list .message-row')]
          .find((el) => el.innerText.includes('回执测试'));
        return !!row && row.dataset.status === 'sent' && Number(row.dataset.seq) > 0;
      })()`, 5000);
      const sentSeq = await tab.evaluate(`(() => {
        const row = [...document.querySelectorAll('#message-list .message-row')]
          .find((el) => el.innerText.includes('回执测试'));
        return row ? Number(row.dataset.seq) : 0;
      })()`);
      run.check("sent message carries a seq", Number.isInteger(sentSeq) && sentSeq >= 1);
      const readerB = await run.openSocket("b");
      try {
        const ack = await readerB.focusDirect(name("a"));
        run.check("the peer acknowledges the message seq", !ack.error && ack.last_read_seq >= sentSeq);
      } finally {
        readerB.close();
      }
      await tab.waitFor(`(() => {
        const row = [...document.querySelectorAll('#message-list .message-row')]
          .find((el) => el.innerText.includes('回执测试'));
        return row?.querySelector('.read-state')?.textContent === '已读';
      })()`, 5000);
      run.check("sender sees the peer read receipt", true);

      const before = focus.frames.length;
      await tab.select(directSelector("c"), `/chat/${name("c")}`);
      await waitUntil(() => focusTarget(focus.frames.slice(before), "direct", name("c")), "switching chats did not focus the new chat");
      run.check("switching conversations focuses the new chat", true);
      run.check("switching conversations sends no read request", reads.paths.length === 0);
    } finally {
      reads.off();
      focus.off();
    }

    await tab.select(memberSelector(live), `/chat/group/${live}`);
    await tab.waitFor("document.querySelectorAll('#message-list .message-row').length > 0");
  });

  await scenario("a skipped push is backfilled with after_seq", async () => {
    await tab.select(directSelector("b"), `/chat/${name("b")}`);
    await tab.waitFor("document.querySelectorAll('#message-list .message-row').length > 0");
    const startSeq = directOf(await conversationsOf("b"), name("a")).last_seq;
    await tab.waitFor(`(() => {
      const rows = [...document.querySelectorAll('#message-list .message-row')];
      return Math.max(0, ...rows.map((row) => Number(row.dataset.seq) || 0)) === ${startSeq};
    })()`, 5000);
    const keys = [randomUUID(), randomUUID(), randomUUID()];
    const sent = [];
    await tab.evaluate(`window.__imDropInbound = (event) => {
      if (window.__imAllowInjected) return false;
      try {
        const frame = JSON.parse(event.data);
        return frame?.type === "message.created"
          && ${JSON.stringify(keys)}.includes(frame.message?.client_message_id);
      } catch {
        return false;
      }
    }`);
    try {
      for (const [index, key] of keys.entries()) {
        const response = await run.request("b", "POST", `/api/conversations/${name("a")}/messages`, {
          body: `缺口消息 ${index + 1}`,
          client_message_id: key,
        });
        sent.push(response.message);
      }
      run.check("server assigned the skipped seqs", sent.every((message, index) => message.seq === startSeq + index + 1));
      const pulled = [];
      const off = tab.on("Network.requestWillBeSent", ({ request }) => {
        try {
          const url = new URL(request.url);
          if (url.pathname === `/api/conversations/${name("b")}/messages` && url.searchParams.has("after_seq")) {
            pulled.push(Number(url.searchParams.get("after_seq")));
          }
        } catch {
          // Ignore non-URL requests.
        }
      });
      try {
        await tab.evaluate(`(() => {
          const socket = window.__imTestSockets.find((candidate) => candidate.readyState === WebSocket.OPEN);
          if (!socket) throw new Error("no open WebSocket to inject into");
          window.__imAllowInjected = true;
          socket.dispatchEvent(new MessageEvent("message", {
            data: JSON.stringify({ type: "message.created", message: ${JSON.stringify(sent[2])} }),
          }));
          window.__imAllowInjected = false;
        })()`);
        for (const message of sent) {
          await tab.waitFor(`document.querySelector('#message-list')?.innerText.includes(${JSON.stringify(message.body)})`, 5000);
        }
        await waitUntil(() => pulled.length > 0, "the client did not request the missing seq range");
      } finally {
        off();
      }
      console.log(`Gap backfill requests: ${JSON.stringify(pulled)}`);
      run.check("a skipped push triggers an after_seq backfill", pulled[0] === startSeq);
      const visible = await tab.evaluate(`(() => {
        const rows = [...document.querySelectorAll('#message-list .message-row')];
        return rows.map((row) => Number(row.dataset.seq)).filter((seq) => Number.isInteger(seq) && seq > 0);
      })()`);
      run.check(
        "the backfilled page closes the gap",
        sent.every((message) => visible.includes(message.seq)),
      );
    } finally {
      await tab.evaluate("window.__imDropInbound = null; window.__imAllowInjected = false");
    }
  });

  await scenario("group receipts, reader changes and sender switching", async () => {
    const reads = trackReadRequests();
    try {
      await tab.select(memberSelector(live), `/chat/group/${live}`);
      await tab.waitFor("document.querySelectorAll('#message-list .message-row').length > 0");
      await tab.setValue("#message-input", "群回执验证");
      await tab.click("#send-message");
      await tab.waitFor(`(${receiptOf("群回执验证")})?.text === '已读 0/2'`, 5000);
      const initial = await tab.evaluate(receiptOf("群回执验证"));
      run.check("group receipt starts at zero", initial.read === "0" && initial.members === "2" && !initial.all);
      await tab.waitFor(`(() => {
        const row = [...document.querySelectorAll('#message-list .message-row')]
          .find((el) => el.innerText.includes('群回执验证'));
        return !!row && Number(row.dataset.seq) > 0;
      })()`, 5000);
      const groupSentSeq = await tab.evaluate(`(() => {
        const row = [...document.querySelectorAll('#message-list .message-row')]
          .find((el) => el.innerText.includes('群回执验证'));
        return row ? Number(row.dataset.seq) : 0;
      })()`);
      run.check("sent group message carries a seq", Number.isInteger(groupSentSeq) && groupSentSeq >= 1);

      const readerB = await run.openSocket("b");
      try {
        const ack = await readerB.focusGroup(live);
        run.check("the group reader acknowledges the message seq", !ack.error && ack.last_read_seq >= groupSentSeq);
      } finally {
        readerB.close();
      }
      await tab.waitFor(`(${receiptOf("群回执验证")})?.text === '已读 1/2'`, 5000);
      run.check("group receipt follows a reader in real time", true);

      await run.request("b", "DELETE", `/api/groups/${live}/members/${name("c")}`);
      await tab.waitFor(`(${receiptOf("群回执验证")})?.text === '全部已读'`, 5000);
      const shrunk = await tab.evaluate(receiptOf("群回执验证"));
      run.check("kicking a non-reader shrinks the denominator", shrunk.read === "1" && shrunk.members === "1" && shrunk.all);
      run.check("group receipts never send read requests", reads.paths.length === 0);

      await tab.setValue("#message-input", "发完就切走");
      await tab.click("#send-message");
      await tab.select(directSelector("c"), `/chat/${name("c")}`);
      await waitUntil(async () => groupOf(await browserConversations(), live)?.unread_count === 0, "sender still has unread after switching away");
      run.check("sending then switching away leaves no sender unread", true);
      await tab.waitFor(`!document.querySelector('#conversation-list ${memberSelector(live)} .unread-badge')`);
      run.check("sender sidebar shows no own unread", true);
    } finally {
      reads.off();
    }
  });

  await scenario("moments feed, like, comment and unread dot", async () => {
    const friendMoment = await run.request("b", "POST", "/api/moments", {
      body: "浏览器里的朋友圈",
      visibility: "friends",
    });
    await run.request("b", "POST", "/api/moments", {
      body: "浏览器私密动态",
      visibility: "private",
    });
    await tab.waitFor("!!document.querySelector('.nav-dot')");
    run.check("a new friend moment shows the nav dot", true);

    await tab.goto("/moments");
    await tab.waitFor("!!document.querySelector('.moments-card')");
    await tab.waitFor("document.body.innerText.includes('浏览器里的朋友圈')");
    run.check("moments page renders the friend moment", true);
    run.check(
      "moments page hides a friend's private moment",
      !(await tab.evaluate("document.body.innerText.includes('浏览器私密动态')")),
    );

    await tab.evaluate(`(() => {
      const card = [...document.querySelectorAll('.moment-card')]
        .find((el) => el.innerText.includes('浏览器里的朋友圈'));
      if (!card) throw new Error('moment card not found');
      [...card.querySelectorAll('.moment-action')]
        .find((button) => button.textContent.trim().startsWith('赞')).click();
    })()`);
    await tab.waitFor(`(() => {
      const card = [...document.querySelectorAll('.moment-card')]
        .find((el) => el.innerText.includes('浏览器里的朋友圈'));
      return !!card && card.innerText.includes('取消赞');
    })()`);
    run.check("moments like toggles in the UI", true);

    await tab.evaluate(`(() => {
      const card = [...document.querySelectorAll('.moment-card')]
        .find((el) => el.innerText.includes('浏览器里的朋友圈'));
      [...card.querySelectorAll('.moment-action')]
        .find((button) => button.textContent.trim().startsWith('评论')).click();
    })()`);
    await tab.waitFor("!!document.querySelector('.moment-comment-form input')");
    await tab.setValue(".moment-comment-form input", "浏览器评论");
    await tab.evaluate("document.querySelector('.moment-comment-form button[type=submit]').click()");
    await tab.waitFor("document.body.innerText.includes('浏览器评论')");
    run.check("moments comment appears in the UI", true);

    const detail = await run.request("b", "GET", `/api/moments/${friendMoment.id}`);
    run.check(
      "moment like and comment persist",
      detail.moment.like_count === 1
        && detail.comments.some((comment) => comment.body === "浏览器评论"),
    );

    await tab.goto("/");
    await tab.waitFor("!!document.querySelector('#conversation-list .conversation-item')");
    await tab.waitFor("!document.querySelector('.nav-dot')");
    run.check("returning to chat clears the moments dot", true);
  });

  await scenario("same-session new window replaces old chat connection", async () => {
    await tab.select(directSelector("b"), `/chat/${name("b")}`);
    await tab.waitFor("document.querySelectorAll('#message-list .message-row').length > 0");
    const replacement = await browser.tab(run.base);
    const { cookies } = await tab.send("Network.getCookies", { urls: [run.base] });
    const cookie = cookies.find((item) => item.name === "im_session");
    await replacement.send("Network.setCookie", { name: cookie.name, value: cookie.value, url: run.base, httpOnly: true });
    await replacement.goto("/");
    await replacement.waitFor("!!document.querySelector('#conversation-list .conversation-item')");
    await tab.waitFor("!!document.querySelector('#ws-replaced')");
    run.check("replaced composer is disabled", await tab.evaluate("!!document.querySelector('#message-input')?.disabled"));
    run.check("replaced window keeps history", await tab.evaluate("document.querySelectorAll('#message-list .message-row').length > 0"));
    await tab.click("#ws-replaced button");
    await tab.waitFor("location.pathname === '/login' && !!document.querySelector('input[name=username]')");
    run.check("relogin returns to login page", true);
  });
  run.check("no browser runtime exception", tab.exceptions.length === 0);
  if (failures) throw new Error(`${failures} browser scenarios failed`);
}

try { await main(); } catch (error) { console.error(`FAIL ${error.message}`); process.exitCode = 1; } finally { await browser.close(); await run.finish(); }
