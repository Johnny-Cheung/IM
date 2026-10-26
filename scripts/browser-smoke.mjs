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

async function readWhileHiding(pattern, send, unreadSelector) {
  await delay(400);
  let resolveRead;
  const paused = new Promise((resolve) => { resolveRead = resolve; });
  const offRead = tab.on("Fetch.requestPaused", resolveRead);
  await tab.send("Fetch.enable", { patterns: [{ urlPattern: pattern, requestStage: "Request" }] });
  try {
    await send("前台已看到");
    const read = await Promise.race([paused, delay(5000).then(() => { throw new Error("read request was not intercepted"); })]);
    await tab.evaluate("Object.defineProperty(document, 'visibilityState', {configurable:true, get:()=>window.__imHidden ? 'hidden' : 'visible'}); window.__imHidden=true; document.dispatchEvent(new Event('visibilitychange'))");
    await send("后台未读");
    await tab.waitFor(`document.querySelector(${JSON.stringify(unreadSelector)})?.textContent === '1'`);
    let resolveCompleted;
    const completed = new Promise((resolve) => { resolveCompleted = resolve; });
    const offCompleted = tab.on("Network.loadingFinished", (event) => { if (event.requestId === read.networkId) resolveCompleted(); });
    try {
      await tab.send("Fetch.continueRequest", { requestId: read.requestId });
      await Promise.race([completed, delay(5000).then(() => { throw new Error("read request did not finish"); })]);
      await delay(250);
      run.check(`in-flight read preserves background unread (${unreadSelector})`, await tab.evaluate(`document.querySelector(${JSON.stringify(unreadSelector)})?.textContent === '1'`));
      run.check("read request captures viewed message position", Number(new URL(read.request.url).searchParams.get("through_id")) > 0);
    } finally { offCompleted(); }
  } finally {
    offRead();
    await tab.send("Fetch.disable");
    await tab.evaluate("window.__imHidden=false; document.dispatchEvent(new Event('visibilitychange'))");
  }
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
    window.WebSocket = class extends WebSocket {
      constructor(...args) { super(...args); window.__imTestSockets.push(this); }
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
    run.check("group has no read receipt", !await tab.evaluate("!!document.querySelector('#message-list .read-state')"));
    await readWhileHiding(
      `*/api/groups/${live}/read*`,
      (body) => run.request("b", "POST", `/api/groups/${live}/messages`, { body, client_message_id: randomUUID() }),
      `#conversation-list ${memberSelector(live)} .unread-badge`,
    );
    await tab.waitFor(`!document.querySelector('#conversation-list ${memberSelector(live)} .unread-badge')`);
    run.check("returning to foreground clears unread", true);
    for (let index = 0; index < 32; index += 1) await run.request("b", "POST", `/api/groups/${live}/messages`, { body: `历史 ${index}`, client_message_id: randomUUID() });
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

  await scenario("private read in flight cannot consume unseen messages", async () => {
    await tab.select(directSelector("b"), `/chat/${name("b")}`);
    const unread = `#conversation-list ${directSelector("b")} .unread-badge`;
    await readWhileHiding(
      `*/api/conversations/${name("b")}/read*`,
      (body) => run.request("b", "POST", `/api/conversations/${name("a")}/messages`, { body, client_message_id: randomUUID() }),
      unread,
    );
    await tab.waitFor(`!document.querySelector(${JSON.stringify(unread)})`);
    run.check("private foreground restoration clears unread", true);
    await tab.select(memberSelector(live), `/chat/group/${live}`);
    await tab.waitFor("document.querySelectorAll('#message-list .message-row').length > 0");
  });

  await scenario("same-session new window replaces old chat connection", async () => {
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
