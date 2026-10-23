#!/usr/bin/env node
// Seeds demo accounts, friendships, groups and messages for the v0.6 group chat.
// Safe to re-run: existing users, friendships, groups and seeded conversations
// are reused instead of duplicated.

import { randomUUID } from "node:crypto";

const BASE_URL = process.env.BASE_URL ?? "http://10.12.75.191:8198";
const PASSWORD = process.env.DEMO_PASSWORD ?? "Demo123456";

const USERS = [
  { username: "demo_alice", nickname: "Alice" },
  { username: "demo_bob", nickname: "Bob" },
  { username: "demo_carol", nickname: "Carol" },
  { username: "demo_dave", nickname: "Dave" },
];

const cookies = new Map();

class ApiError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

function sessionCookie(response) {
  const values = typeof response.headers.getSetCookie === "function"
    ? response.headers.getSetCookie()
    : [response.headers.get("set-cookie")].filter(Boolean);
  const pair = values
    .flatMap((value) => String(value).split(/,(?=[^;]+=)/))
    .map((value) => value.trim().split(";")[0])
    .find((value) => value.startsWith("im_session="));
  return pair ?? null;
}

async function request(username, method, path, body) {
  const headers = {};
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  const cookie = cookies.get(username);
  if (cookie) {
    headers.Cookie = cookie;
  }

  const response = await fetch(`${BASE_URL}${path}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const nextCookie = sessionCookie(response);
  if (nextCookie) {
    cookies.set(username, nextCookie);
  }

  const text = await response.text();
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    payload = text;
  }
  if (!response.ok) {
    throw new ApiError(
      response.status,
      payload?.error?.code ?? "request_failed",
      payload?.error?.message ?? `HTTP ${response.status}`,
    );
  }
  return payload;
}

async function ensureUser({ username, nickname }) {
  try {
    await request(username, "POST", "/api/auth/register", {
      username,
      nickname,
      password: PASSWORD,
    });
    console.log(`created user ${username}`);
    return;
  } catch (error) {
    if (!(error instanceof ApiError) || error.status !== 409) {
      throw error;
    }
  }

  try {
    await request(username, "POST", "/api/auth/login", {
      username,
      password: PASSWORD,
    });
    console.log(`reused user ${username}`);
  } catch (error) {
    if (error instanceof ApiError && error.status === 401) {
      throw new Error(
        `用户名 ${username} 已存在，但演示密码不匹配；请更换用户名或先修改其密码`,
      );
    }
    throw error;
  }
}

async function ensureFriendship(first, second) {
  const friends = await request(first, "GET", "/api/friends");
  if (friends.friends.some((friend) =>
    friend.username.toLowerCase() === second.toLowerCase())) {
    return;
  }

  try {
    await request(first, "POST", "/api/friend-requests", { username: second });
  } catch (error) {
    if (!(error instanceof ApiError) || error.status !== 409) {
      throw error;
    }
  }

  const requests = await request(second, "GET", "/api/friend-requests");
  const incoming = requests.incoming.find((item) =>
    item.user.username.toLowerCase() === first.toLowerCase());
  if (incoming) {
    await request(second, "POST", `/api/friend-requests/${incoming.id}/accept`);
  }

  const refreshed = await request(first, "GET", "/api/friends");
  if (!refreshed.friends.some((friend) =>
    friend.username.toLowerCase() === second.toLowerCase())) {
    throw new Error(`未能建立好友关系：${first} -> ${second}`);
  }
}

async function ensureGroup(owner, name, memberUsernames) {
  const list = await request(owner, "GET", "/api/conversations");
  const existing = list.conversations.find((conversation) =>
    conversation.type === "group" && conversation.group.name === name);
  let detail;
  if (existing) {
    detail = await request(owner, "GET", `/api/groups/${existing.group.id}`);
    console.log(`reused group ${name} (#${detail.group.id})`);
  } else {
    detail = await request(owner, "POST", "/api/groups", {
      name,
      member_usernames: memberUsernames,
    });
    console.log(`created group ${name} (#${detail.group.id})`);
  }

  const missing = memberUsernames.filter((username) =>
    !detail.members.some((member) =>
      member.user.username.toLowerCase() === username.toLowerCase()));
  if (missing.length > 0) {
    detail = await request(owner, "POST", `/api/groups/${detail.group.id}/members`, {
      usernames: missing,
    });
    console.log(`invited ${missing.join(", ")} to ${name}`);
  }
  return detail;
}

async function seedGroupMessages(owner, detail, messages) {
  const page = await request(
    owner,
    "GET",
    `/api/groups/${detail.group.id}/messages?limit=1`,
  );
  if (page.messages.length > 0) {
    console.log(`kept existing messages in ${detail.group.name}`);
    return;
  }
  for (const message of messages) {
    await request(message.from, "POST", `/api/groups/${detail.group.id}/messages`, {
      body: message.body,
      client_message_id: randomUUID(),
    });
  }
  console.log(`seeded ${messages.length} messages in ${detail.group.name}`);
}

async function seedDirectMessages() {
  const conversations = await request("demo_alice", "GET", "/api/conversations");
  const direct = conversations.conversations.find((conversation) =>
    conversation.type === "direct"
    && conversation.user.username.toLowerCase() === "demo_bob");
  if (direct?.last_message) {
    console.log("kept existing demo direct conversation");
    return;
  }
  await request("demo_alice", "POST", "/api/conversations/demo_bob/messages", {
    body: "晚上一起吃饭吗？",
    client_message_id: randomUUID(),
  });
  await request("demo_bob", "POST", "/api/conversations/demo_alice/messages", {
    body: "好，六点老地方见。",
    client_message_id: randomUUID(),
  });
  console.log("seeded demo direct conversation");
}

const FRIEND_PAIRS = [
  ["demo_alice", "demo_bob"],
  ["demo_alice", "demo_carol"],
  ["demo_alice", "demo_dave"],
  ["demo_bob", "demo_carol"],
  ["demo_bob", "demo_dave"],
  ["demo_carol", "demo_dave"],
];

const GROUPS = [
  {
    owner: "demo_alice",
    name: "周末出游",
    members: ["demo_bob", "demo_carol"],
    messages: [
      { from: "demo_alice", body: "周六去爬山吗？天气预报说是晴天。" },
      { from: "demo_bob", body: "可以，我带上相机。" },
      { from: "demo_carol", body: "我也去！几点集合？" },
      { from: "demo_alice", body: "早上 8 点地铁站见。" },
    ],
  },
  {
    owner: "demo_bob",
    name: "项目讨论",
    members: ["demo_alice", "demo_carol"],
    messages: [
      { from: "demo_bob", body: "v0.6 的群聊已经上线了。" },
      { from: "demo_alice", body: "我试了一下，会话列表里能看到群聊。" },
      { from: "demo_carol", body: "未读数字和群信息面板也正常。" },
      { from: "demo_bob", body: "大家有空帮忙回归一下邀请和退群。" },
    ],
  },
  {
    owner: "demo_carol",
    name: "电影搭子",
    members: ["demo_alice", "demo_bob", "demo_dave"],
    messages: [
      { from: "demo_carol", body: "这周有人想看新上映的科幻片吗？" },
      { from: "demo_dave", body: "我想看，周六下午怎么样？" },
      { from: "demo_bob", body: "我周六下午有空。" },
      { from: "demo_alice", body: "算我一个。" },
    ],
  },
];

async function main() {
  console.log(`seeding ${BASE_URL}`);
  for (const user of USERS) {
    await ensureUser(user);
  }
  for (const [first, second] of FRIEND_PAIRS) {
    await ensureFriendship(first, second);
  }

  for (const group of GROUPS) {
    const detail = await ensureGroup(group.owner, group.name, group.members);
    await seedGroupMessages(group.owner, detail, group.messages);
  }
  await seedDirectMessages();

  console.log("\n演示账号：");
  for (const user of USERS) {
    console.log(`  ${user.username} / ${PASSWORD}（${user.nickname}）`);
  }
  console.log("\n演示群聊：");
  for (const group of GROUPS) {
    const list = await request(group.owner, "GET", "/api/conversations");
    const conversation = list.conversations.find((item) =>
      item.type === "group"
      && item.group.id
      && item.group.name === group.name
      && item.group.owner.username.toLowerCase() === group.owner);
    if (conversation) {
      console.log(
        `  ${group.name}（群主 ${group.owner}，未读 ${conversation.unread_count}）`,
      );
    }
  }
  console.log(`\n直接登录 ${BASE_URL}/login 即可查看。`);
}

main().catch((error) => {
  console.error(`\nseed failed: ${error.message}`);
  process.exitCode = 1;
});
