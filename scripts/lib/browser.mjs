import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { delay } from "./test-client.mjs";

export class Browser {
  constructor() {
    this.nextID = 0;
    this.pending = new Map();
    this.listeners = new Map();
    this.contexts = [];
  }

  async start() {
    this.profile = await mkdtemp(join(tmpdir(), "im-browser-"));
    const executable = process.env.CHROME_PATH ?? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
    this.process = spawn(executable, [
      "--headless", "--remote-debugging-port=0", "--remote-debugging-address=127.0.0.1",
      `--user-data-dir=${this.profile}`, "--no-first-run", "--no-default-browser-check",
      "--disable-background-networking", "--disable-extensions", "--disable-sync", "about:blank",
    ], { stdio: "ignore" });
    let spawnError;
    this.process.on("error", (error) => { spawnError = error; });
    let endpoint;
    for (let index = 0; index < 80; index += 1) {
      if (spawnError) throw spawnError;
      if (this.process.exitCode !== null) throw new Error("Chrome exited before debugging was ready");
      try {
        const [port, path] = (await readFile(join(this.profile, "DevToolsActivePort"), "utf8")).trim().split("\n");
        endpoint = `ws://127.0.0.1:${port}${path}`;
        break;
      } catch { await delay(100); }
    }
    if (!endpoint) throw new Error("Chrome debugging endpoint not ready");
    this.socket = new WebSocket(endpoint);
    await new Promise((resolve, reject) => {
      this.socket.addEventListener("open", resolve, { once: true });
      this.socket.addEventListener("error", reject, { once: true });
    });
    this.socket.addEventListener("message", ({ data }) => {
      const message = JSON.parse(String(data));
      if (message.id) {
        const entry = this.pending.get(message.id);
        if (!entry) return;
        this.pending.delete(message.id);
        clearTimeout(entry.timer);
        if (message.error) entry.reject(new Error(message.error.message));
        else entry.resolve(message.result);
      } else {
        for (const listener of this.listeners.get(`${message.sessionId}:${message.method}`) ?? []) {
          listener(message.params);
        }
      }
    });
    return this;
  }

  send(method, params = {}, sessionId) {
    return new Promise((resolve, reject) => {
      const id = ++this.nextID;
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`CDP timed out: ${method}`)); }, 20000);
      this.pending.set(id, { resolve, reject, timer });
      this.socket.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) }));
    });
  }

  on(sessionID, method, listener) {
    const key = `${sessionID}:${method}`;
    const listeners = this.listeners.get(key) ?? new Set();
    listeners.add(listener);
    this.listeners.set(key, listeners);
    return () => listeners.delete(listener);
  }

  async tab(base) {
    const { browserContextId } = await this.send("Target.createBrowserContext");
    this.contexts.push(browserContextId);
    const { targetId } = await this.send("Target.createTarget", { url: "about:blank", browserContextId });
    const { sessionId } = await this.send("Target.attachToTarget", { targetId, flatten: true });
    const tab = new Tab(this, sessionId, base);
    await tab.send("Page.enable");
    await tab.send("Runtime.enable");
    await tab.send("Network.enable");
    await tab.viewport(1280, 900);
    return tab;
  }

  async close() {
    for (const id of this.contexts) await this.send("Target.disposeBrowserContext", { browserContextId: id }).catch(() => undefined);
    if (this.socket?.readyState === WebSocket.OPEN) {
      await this.send("Browser.close").catch(() => undefined);
      this.socket.close();
    }
    if (this.process && this.process.exitCode === null) {
      const exited = new Promise((resolve) => this.process.once("exit", resolve));
      this.process.kill();
      await Promise.race([exited, delay(2000)]);
    }
    for (const { reject, timer } of this.pending.values()) { clearTimeout(timer); reject(new Error("Browser closed")); }
    this.pending.clear();
    if (this.profile) await rm(this.profile, { recursive: true, force: true });
  }
}

class Tab {
  constructor(browser, sessionID, base) {
    this.browser = browser;
    this.sessionID = sessionID;
    this.base = base;
    this.exceptions = [];
    browser.on(sessionID, "Runtime.exceptionThrown", (event) => this.exceptions.push(event.exceptionDetails.text));
  }

  send(method, params = {}) { return this.browser.send(method, params, this.sessionID); }
  on(method, listener) { return this.browser.on(this.sessionID, method, listener); }

  async evaluate(expression) {
    const result = await this.send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    if (result.exceptionDetails) throw new Error(`Browser evaluation: ${result.exceptionDetails.text}`);
    return result.result.value;
  }

  async waitFor(expression, timeout = 10000) {
    const end = Date.now() + timeout;
    while (Date.now() < end) {
      if (await this.evaluate(expression)) return;
      await delay(80);
    }
    throw new Error(`Browser condition timed out: ${expression}`);
  }

  async goto(path) {
    await this.send("Page.navigate", { url: this.base + path });
    await this.waitFor(`location.pathname === ${JSON.stringify(path.split("?")[0])} && document.readyState === 'complete'`);
  }

  async setValue(selector, value) {
    await this.evaluate(`(() => {
      const el = document.querySelector(${JSON.stringify(selector)});
      if (!el) throw new Error('Input not found');
      const proto = el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
      Object.getOwnPropertyDescriptor(proto, 'value').set.call(el, ${JSON.stringify(value)});
      el.dispatchEvent(new Event('input', {bubbles:true}));
    })()`);
  }

  async click(selector) {
    await this.waitFor(`(() => { const el = document.querySelector(${JSON.stringify(selector)}); return !!el && !el.disabled; })()`);
    await this.evaluate(`document.querySelector(${JSON.stringify(selector)}).click()`);
  }

  async login(username, password) {
    await this.goto("/login");
    await this.waitFor("!!document.querySelector('input[name=username]')");
    await this.setValue("input[name=username]", username);
    await this.setValue("input[name=password]", password);
    await this.click("button[type=submit]");
    await this.waitFor("!!document.querySelector('#conversation-list .conversation-item')");
  }

  async select(selector, path) {
    await this.click(`#conversation-list ${selector}`);
    await this.waitFor(`location.pathname === ${JSON.stringify(path)} && !!document.querySelector('#message-input') && !document.querySelector('#message-input').disabled && !document.querySelector('#message-list').innerText.includes('正在')`);
  }

  async viewport(width, height) {
    await this.send("Emulation.setDeviceMetricsOverride", { width, height, deviceScaleFactor: 1, mobile: width < 760 });
  }

  async shot(path) {
    const { data } = await this.send("Page.captureScreenshot", { format: "png" });
    await writeFile(path, Buffer.from(data, "base64"));
  }
}
