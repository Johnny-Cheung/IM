#!/usr/bin/env node
import { randomUUID } from "node:crypto";
import { TestRun } from "./lib/test-client.mjs";

const run = new TestRun("api-smoke");
const groupOf = (list, id) => list.conversations.find((item) => item.type === "group" && item.group.id === id);
const directOf = (list, name) => list.conversations.find((item) => item.type === "direct" && item.user.username === name);
const list = (who) => run.request(who, "GET", "/api/conversations");
const sendGroup = (who, id, body, key = randomUUID()) => run.request(who, "POST", `/api/groups/${id}/messages`, { body, client_message_id: key });
const directPath = (who) => `/api/conversations/${run.users.get(who).username}`;

async function main() {
  console.log(`API smoke ${run.id}: ${run.base}`);
  for (const path of ["/health", "/ready"]) {
    const response = await fetch(run.base + path, { signal: AbortSignal.timeout(10000) });
    run.check(`${path} is healthy`, response.status === 200 && (await response.json()).status === "ok");
  }
  const page = await fetch(run.base, { signal: AbortSignal.timeout(10000) });
  run.check("index served", page.status === 200);
  const assets = [...(await page.text()).matchAll(/(?:src|href)="(\/assets\/[^"\s]+)"/g)].map((match) => match[1]);
  run.check("built assets referenced", assets.length >= 2);
  for (const asset of assets) {
    run.check(`asset ${asset}`, (await fetch(run.base + asset, { signal: AbortSignal.timeout(10000) })).status === 200);
  }
  await run.expectError("anonymous conversations rejected", null, "GET", "/api/conversations", undefined, 401, "unauthenticated");
  for (const key of ["a", "b", "c", "d"]) await run.register(key);
  await run.befriend("a", "b");
  await run.befriend("a", "c");
  await run.befriend("b", "c");
  const friends = await run.request("a", "GET", "/api/friends");
  run.check("accepted friends listed", friends.friends.length === 2);
  const accepted = await run.request("b", "GET", "/api/friend-requests");
  run.check("accepted request retained", accepted.history.some((item) => item.status === "accepted"));
  await run.request("a", "POST", "/api/friend-requests", { username: run.users.get("d").username });
  const pending = await run.request("d", "GET", "/api/friend-requests");
  await run.request("d", "POST", `/api/friend-requests/${pending.incoming[0].id}/reject`);
  const rejected = await run.request("a", "GET", "/api/friend-requests");
  run.check("rejected outgoing request retained", rejected.history.some((item) => item.status === "rejected"));

  const directID = randomUUID();
  const first = await run.request("a", "POST", `${directPath("b")}/messages`, { body: "私聊第一条\n🙂", client_message_id: directID });
  const duplicate = await run.request("a", "POST", `${directPath("b")}/messages`, { body: "私聊第一条\n🙂", client_message_id: directID });
  run.check("private send retry is idempotent", first.message.id === duplicate.message.id);
  run.check("private unread increments", directOf(await list("b"), run.users.get("a").username).unread_count === 1);
  await run.request("b", "POST", `${directPath("a")}/read`);
  run.check("private unread clears", directOf(await list("b"), run.users.get("a").username).unread_count === 0);
  run.check("peer read receipt persisted", directOf(await list("a"), run.users.get("b").username).peer_last_read_message_id >= first.message.id);
  await run.expectError("non-friend send denied", "d", "POST", `${directPath("a")}/messages`, { body: "no", client_message_id: randomUUID() }, 403, "not_friends");
  const outsider = await run.request("d", "GET", `${directPath("a")}/messages`);
  run.check("outsider sees only their own empty pair", outsider.messages.length === 0);

  for (let index = 0; index < 32; index += 1) {
    await run.request("a", "POST", `${directPath("b")}/messages`, { body: `private page ${index}`, client_message_id: randomUUID() });
  }
  const newest = await run.request("b", "GET", `${directPath("a")}/messages?limit=30`);
  const older = await run.request("b", "GET", `${directPath("a")}/messages?before_id=${newest.messages[0].id}&limit=30`);
  run.check("private history pages without overlap", newest.has_more && newest.messages.length === 30 && older.messages.length === 3 && older.messages.at(-1).id < newest.messages[0].id);
  const directAfter = await run.request("b", "GET", `${directPath("a")}/messages?after_id=${first.message.id}&limit=50`);
  run.check("private reconnect cursor catches up", directAfter.messages.length === 32);
  await run.request("b", "POST", `${directPath("a")}/read?through_id=${first.message.id}`);
  run.check("bounded private read preserves newer unread", directOf(await list("b"), run.users.get("a").username).unread_count === 32);
  const directLatest = newest.messages.at(-1).id;
  await run.request("b", "POST", `${directPath("a")}/read?through_id=${directLatest}`);
  const directOldRead = await run.request("b", "POST", `${directPath("a")}/read?through_id=${first.message.id}`);
  run.check("private read position cannot rewind", directOldRead.last_read_message_id === directLatest && directOf(await list("b"), run.users.get("a").username).unread_count === 0);
  await run.expectError("invalid private read bound rejected", "b", "POST", `${directPath("a")}/read?through_id=-1`, undefined, 400, "invalid_request");

  await run.expectError("group needs members", "a", "POST", "/api/groups", { name: "empty", member_usernames: [] }, 400, "invalid_members");
  await run.expectError("group needs valid name", "a", "POST", "/api/groups", { name: "群".repeat(31), member_usernames: [run.users.get("b").username] }, 400, "invalid_group_name");
  await run.expectError("group invite requires own friendship", "a", "POST", "/api/groups", { name: "nonfriend", member_usernames: [run.users.get("d").username] }, 403, "not_friends");
  const id = await run.group("a", "验证群", ["b"]);
  run.check("member sees group", groupOf(await list("b"), id).group.member_count === 2);
  const messageKey = randomUUID();
  const groupFirst = await sendGroup("a", id, "入群前的历史", messageKey);
  const repeated = await sendGroup("a", id, "入群前的历史", messageKey);
  run.check("group retry is idempotent", groupFirst.message.id === repeated.message.id);
  run.check("group sender has no own unread", groupOf(await list("a"), id).unread_count === 0);
  run.check("group receiver unread increments", groupOf(await list("b"), id).unread_count === 1);
  await run.request("b", "POST", `/api/groups/${id}/read`);
  run.check("group read persists", groupOf(await list("b"), id).unread_count === 0);
  await run.request("b", "POST", `/api/groups/${id}/members`, { usernames: [run.users.get("c").username] });
  run.check("ordinary member can invite their friend", groupOf(await list("c"), id).group.member_count === 3);
  run.check("new member history starts read", groupOf(await list("c"), id).unread_count === 0);
  const oldHistory = await run.request("c", "GET", `/api/groups/${id}/messages`);
  run.check("new member sees prejoin history", oldHistory.messages[0].body === "入群前的历史");
  await run.request("a", "POST", `/api/groups/${id}/members`, { usernames: [run.users.get("c").username] });
  run.check("duplicate invite ignored", groupOf(await list("a"), id).group.member_count === 3);
  await run.expectError("outsider group read denied", "d", "GET", `/api/groups/${id}`, undefined, 404, "group_not_found");
  await run.expectError("ordinary member rename denied", "b", "PATCH", `/api/groups/${id}`, { name: "no" }, 403, "not_owner");
  await run.expectError("ordinary member kick denied", "b", "DELETE", `/api/groups/${id}/members/${run.users.get("c").username}`, undefined, 403, "not_owner");
  await run.request("a", "PATCH", `/api/groups/${id}`, { name: "已改名的验证群" });
  run.check("rename persisted for all members", groupOf(await list("b"), id).group.name === "已改名的验证群");
  for (let index = 0; index < 35; index += 1) await sendGroup("a", id, `group page ${index}`);
  const groupPage = await run.request("b", "GET", `/api/groups/${id}/messages?limit=30`);
  const groupOlder = await run.request("b", "GET", `/api/groups/${id}/messages?limit=30&before_id=${groupPage.messages[0].id}`);
  run.check("group history pages without overlap", groupPage.has_more && groupPage.messages.length === 30 && groupOlder.messages.length === 6 && groupOlder.messages.at(-1).id < groupPage.messages[0].id);
  const groupAfter = await run.request("b", "GET", `/api/groups/${id}/messages?limit=50&after_id=${groupFirst.message.id}`);
  run.check("group reconnect cursor catches up", groupAfter.messages.length === 35);
  await run.request("b", "POST", `/api/groups/${id}/read?through_id=${groupFirst.message.id}`);
  run.check("bounded group read preserves newer unread", groupOf(await list("b"), id).unread_count === 35);
  const groupLatest = groupPage.messages.at(-1).id;
  await run.request("b", "POST", `/api/groups/${id}/read?through_id=${groupLatest}`);
  const groupOldRead = await run.request("b", "POST", `/api/groups/${id}/read?through_id=${groupFirst.message.id}`);
  run.check("group read position cannot rewind", groupOldRead.last_read_message_id === groupLatest && groupOf(await list("b"), id).unread_count === 0);
  await run.expectError("invalid group read bound rejected", "b", "POST", `/api/groups/${id}/read?through_id=-1`, undefined, 400, "invalid_request");
  await run.expectError("conflicting group cursors rejected", "a", "GET", `/api/groups/${id}/messages?before_id=10&after_id=1`, undefined, 400, "invalid_request");
  await run.request("a", "DELETE", `/api/groups/${id}/members/${run.users.get("c").username}`);
  await run.expectError("kicked member loses history", "c", "GET", `/api/groups/${id}/messages`, undefined, 404, "group_not_found");
  await run.request("a", "POST", `/api/groups/${id}/members`, { usernames: [run.users.get("c").username] });
  run.check("reinvited member baseline advances", groupOf(await list("c"), id).unread_count === 0);
  await run.request("c", "POST", `/api/groups/${id}/leave`);
  run.check("member can leave", !groupOf(await list("c"), id));
  await run.request("a", "DELETE", `/api/friends/${run.users.get("b").username}`);
  await run.expectError("deleted friend private send denied", "a", "POST", `${directPath("b")}/messages`, { body: "no", client_message_id: randomUUID() }, 403, "not_friends");
  run.check("deleted friend private history retained", (await run.request("a", "GET", `${directPath("b")}/messages`)).messages.length > 0);
  await sendGroup("b", id, "删好友仍可群聊");
  run.check("group membership independent of friendship", true);
  const dissolved = await run.request("a", "POST", `/api/groups/${id}/leave`);
  run.check("owner leaving dissolves group", dissolved.dissolved);
  run.check("dissolved group removed", !groupOf(await list("b"), id));
  await run.expectError("dissolved history denied", "b", "GET", `/api/groups/${id}/messages`, undefined, 404, "group_not_found");
}

try { await main(); } catch (error) { console.error(`FAIL ${error.message}`); process.exitCode = 1; } finally { await run.finish(); }
