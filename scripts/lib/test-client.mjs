import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

export const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const sameUsername = (first, second) =>
  String(first).toLocaleLowerCase() === String(second).toLocaleLowerCase();

export class ApiError extends Error {
  constructor(status, code) {
    super(`HTTP ${status}: ${code}`);
    this.status = status;
    this.code = code;
  }
}

export class TestRun {
  constructor(label) {
    this.base = new URL(process.env.BASE_URL ?? "http://10.12.75.191:8199").origin;
    this.id = randomBytes(4).toString("hex");
    this.prefix = `v7_${this.id}`;
    this.password = randomBytes(24).toString("hex");
    this.users = new Map();
    this.createdUsers = new Set();
    this.cookies = new Map();
    this.groups = [];
    this.checks = 0;
    this.reportDir = process.env.REPORT_DIR ?? join(tmpdir(), `im-${label}-${this.id}`);
  }

  check(name, condition) {
    assert.ok(condition, name);
    this.checks += 1;
    console.log(`PASS ${name}`);
  }

  async send(user, method, path, body) {
    const headers = {};
    if (body !== undefined) headers["Content-Type"] = "application/json";
    if (this.cookies.has(user)) headers.Cookie = this.cookies.get(user);
    const response = await fetch(this.base + path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(15000),
    });
    const cookie = response.headers.getSetCookie()
      .map((value) => value.split(";")[0])
      .find((value) => value.startsWith("im_session="));
    if (cookie && user) this.cookies.set(user, cookie);
    const text = await response.text();
    let payload;
    try { payload = text ? JSON.parse(text) : undefined; } catch { payload = text; }
    return { status: response.status, ok: response.ok, payload };
  }

  async request(user, method, path, body) {
    const { status, ok, payload } = await this.send(user, method, path, body);
    if (!ok) throw new ApiError(status, payload?.error?.code ?? "request_failed");
    return payload;
  }

  // Deleted routes and other status-only checks have no JSON error code.
  async expectStatus(name, user, method, path, status, body) {
    const response = await this.send(user, method, path, body);
    assert.equal(response.status, status, `${name}: got HTTP ${response.status}`);
    this.check(name, true);
    return response.payload;
  }

  async expectError(name, user, method, path, body, status, code) {
    await assert.rejects(
      () => this.request(user, method, path, body),
      (error) => error instanceof ApiError && error.status === status && error.code === code,
      name,
    );
    this.check(name, true);
  }

  async register(key) {
    const user = { username: `${this.prefix}_${key}`, nickname: `验证 ${key}` };
    this.users.set(key, user);
    await this.request(key, "POST", "/api/auth/register", { ...user, password: this.password });
    this.createdUsers.add(user.username);
    await this.save();
    this.check(`register ${key}`, true);
    return user;
  }

  async befriend(first, second) {
    const username = this.users.get(second).username;
    await this.request(first, "POST", "/api/friend-requests", { username });
    const requests = await this.request(second, "GET", "/api/friend-requests");
    const pending = requests.incoming.find((item) => item.user.username === this.users.get(first).username);
    assert.ok(pending, "incoming friend request exists");
    await this.request(second, "POST", `/api/friend-requests/${pending.id}/accept`);
  }

  async group(owner, name, members) {
    const detail = await this.request(owner, "POST", "/api/groups", {
      name,
      member_usernames: members.map((key) => this.users.get(key).username),
    });
    this.groups.push(detail.group.id);
    await this.save();
    return detail.group.id;
  }

  async openSocket(user) {
    const socket = new WebSocket(`${this.base.replace(/^http/, "ws")}/api/ws`, {
      headers: { Cookie: this.cookies.get(user) },
    });
    const events = [];
    socket.addEventListener("message", (event) => {
      try { events.push(JSON.parse(event.data)); } catch { /* ignore non-JSON frames */ }
    });
    await new Promise((resolve, reject) => {
      socket.addEventListener("open", resolve, { once: true });
      socket.addEventListener("error", () => reject(new Error(`websocket for ${user} failed to open`)), { once: true });
    });
    return {
      events,
      send: (frame) => socket.send(JSON.stringify(frame)),
      async waitFor(predicate, message, from = 0, timeout = 5000) {
        const deadline = Date.now() + timeout;
        while (Date.now() < deadline) {
          const found = events.slice(from).find(predicate);
          if (found) return found;
          await delay(25);
        }
        throw new Error(message);
      },
      async focusDirect(username) {
        const from = events.length;
        socket.send(JSON.stringify({ type: "focus", kind: "direct", username }));
        return this.waitFor(
          (event) => event.type === "focus.ack" && event.kind === "direct"
            && sameUsername(event.username, username),
          `direct focus on ${username} was not acknowledged`,
          from,
        );
      },
      async focusGroup(groupID) {
        const from = events.length;
        socket.send(JSON.stringify({ type: "focus", kind: "group", group_id: groupID }));
        return this.waitFor(
          (event) => event.type === "focus.ack" && event.kind === "group" && event.group_id === groupID,
          `group focus on ${groupID} was not acknowledged`,
          from,
        );
      },
      blur: () => socket.send(JSON.stringify({ type: "blur" })),
      close: () => socket.close(),
    };
  }

  async save() {
    await mkdir(this.reportDir, { recursive: true });
    await writeFile(join(this.reportDir, "manifest.json"), JSON.stringify({
      base: this.base,
      run_id: this.id,
      usernames: [...this.createdUsers],
      group_ids: this.groups,
      checks: this.checks,
    }, null, 2), { mode: 0o600 });
  }

  async finish() {
    for (const key of this.cookies.keys()) {
      await this.request(key, "POST", "/api/auth/logout").catch(() => undefined);
    }
    await this.save();
    console.log(`Checks: ${this.checks}; cleanup manifest: ${join(this.reportDir, "manifest.json")}`);
  }
}
