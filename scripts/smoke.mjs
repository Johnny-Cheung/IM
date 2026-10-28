#!/usr/bin/env node
import { randomUUID } from "node:crypto";
import { delay, TestRun } from "./lib/test-client.mjs";

const run = new TestRun("api-smoke");
const groupOf = (list, id) => list.conversations.find((item) => item.type === "group" && item.group.id === id);
const directOf = (list, name) => list.conversations.find((item) => item.type === "direct" && item.user.username === name);
const list = (who) => run.request(who, "GET", "/api/conversations");
const bootstrapOf = (who) => run.request(who, "POST", "/api/bootstrap", {});
const sendGroup = (who, id, body, key = randomUUID()) => run.request(who, "POST", `/api/groups/${id}/messages`, { body, client_message_id: key });
const directPath = (who) => `/api/conversations/${run.users.get(who).username}`;
const readStats = (who, id, messageID) => run.request(who, "GET", `/api/groups/${id}/read-stats?message_id=${messageID}`);
const groupReadThrough = (who, id, throughID) => run.request(who, "POST", `/api/groups/${id}/read?through_id=${throughID}`);

async function until(condition, message, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await delay(100);
  }
  throw new Error(message);
}

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
  for (const key of ["a", "b", "c", "d", "e", "f", "g"]) await run.register(key);
  await run.befriend("a", "b");
  await run.befriend("a", "c");
  await run.befriend("b", "c");
  await run.befriend("e", "f");
  await run.befriend("e", "g");
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

  const batchMessage = await run.request("a", "POST", `${directPath("b")}/messages`, {
    body: "批量已读",
    client_message_id: randomUUID(),
  });
  const batch = await run.request("b", "POST", "/api/read", {
    reads: [
      { username: run.users.get("a").username, through_id: batchMessage.message.id },
      { username: `${run.prefix}_missing`, through_id: batchMessage.message.id },
      { username: run.users.get("b").username, through_id: batchMessage.message.id },
    ],
  });
  run.check(
    "batch read advances valid item",
    batch.reads[0].advanced === true
      && batch.reads[0].last_read_message_id === batchMessage.message.id,
  );
  run.check("batch read reports missing peer", batch.reads[1].error === "user_not_found");
  run.check("batch read reports self", batch.reads[2].error === "cannot_message_self");
  run.check("batch read clears unread", directOf(await list("b"), run.users.get("a").username).unread_count === 0);
  run.check(
    "batch read persists receipt",
    directOf(await list("a"), run.users.get("b").username).peer_last_read_message_id >= batchMessage.message.id,
  );
  const batchRewind = await run.request("b", "POST", "/api/read", {
    reads: [{ username: run.users.get("a").username, through_id: first.message.id }],
  });
  run.check(
    "batch read cannot rewind",
    batchRewind.reads[0].advanced === false
      && batchRewind.reads[0].last_read_message_id === batchMessage.message.id,
  );

  await run.expectError("group needs members", "a", "POST", "/api/groups", { name: "empty", member_usernames: [] }, 400, "invalid_members");
  await run.expectError("group needs valid name", "a", "POST", "/api/groups", { name: "群".repeat(31), member_usernames: [run.users.get("b").username] }, 400, "invalid_group_name");
  await run.expectError("group invite requires own friendship", "a", "POST", "/api/groups", { name: "nonfriend", member_usernames: [run.users.get("d").username] }, 403, "not_friends");
  const id = await run.group("a", "验证群", ["b"]);
  run.check("member sees group", groupOf(await list("b"), id).group.member_count === 2);
  const messageKey = randomUUID();
  const groupFirst = await sendGroup("a", id, "入群前的历史", messageKey);
  const repeated = await sendGroup("a", id, "入群前的历史", messageKey);
  run.check("group retry is idempotent", groupFirst.message.id === repeated.message.id);
  // v1.7 advances the sender's own position asynchronously after accept.
  await until(async () => groupOf(await list("a"), id)?.unread_count === 0, "group sender unread did not settle");
  run.check("group sender has no own unread", true);
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

  const statsGroup = await run.group("e", "已读验证群", ["f", "g"]);
  const statsPath = `/api/groups/${statsGroup}/read-stats`;
  const readFirst = await sendGroup("e", statsGroup, "已读第一条");
  await until(async () => groupOf(await list("e"), statsGroup)?.unread_count === 0, "sender unread did not settle");
  run.check("group sender never carries own unread", true);
  const peerF = groupOf(await list("f"), statsGroup);
  const peerG = groupOf(await list("g"), statsGroup);
  run.check(
    "group unread matches for peers",
    peerF.unread_count === 1 && peerG.unread_count === 1,
  );
  run.check(
    "bootstrap group unread matches conversations",
    groupOf(await bootstrapOf("f"), statsGroup).unread_count === peerF.unread_count,
  );
  run.check(
    "group sender has no own unread in bootstrap",
    groupOf(await bootstrapOf("e"), statsGroup).unread_count === 0,
  );
  const ownStats = await readStats("e", statsGroup, readFirst.message.id);
  run.check(
    "read-stats starts at zero",
    ownStats.message_id === readFirst.message.id
      && ownStats.read_count === 0 && ownStats.member_count === 2 && ownStats.all_read === false,
  );
  await run.expectError("read-stats needs a message id", "e", "GET", statsPath, undefined, 400, "invalid_request");
  await run.expectError("read-stats rejects zero message id", "e", "GET", `${statsPath}?message_id=0`, undefined, 400, "invalid_request");
  await run.expectError("read-stats rejects malformed message id", "e", "GET", `${statsPath}?message_id=abc`, undefined, 400, "invalid_request");
  await run.expectError("outsider cannot read stats or probe messages", "d", "GET", `${statsPath}?message_id=${readFirst.message.id}`, undefined, 404, "group_not_found");
  await groupReadThrough("f", statsGroup, readFirst.message.id);
  const oneReader = await readStats("e", statsGroup, readFirst.message.id);
  run.check(
    "read-stats counts a partial reader",
    oneReader.read_count === 1 && oneReader.member_count === 2 && oneReader.all_read === false,
  );
  await groupReadThrough("g", statsGroup, readFirst.message.id);
  const everyone = await readStats("e", statsGroup, readFirst.message.id);
  run.check(
    "read-stats reports all read",
    everyone.read_count === 2 && everyone.member_count === 2 && everyone.all_read === true,
  );
  const otherMessage = await sendGroup("g", statsGroup, "他人的消息");
  await run.expectError("read-stats rejects another member's message", "e", "GET", `${statsPath}?message_id=${otherMessage.message.id}`, undefined, 403, "not_your_message");
  const readSecond = await sendGroup("e", statsGroup, "已读第二条");
  await until(async () => groupOf(await list("e"), statsGroup)?.unread_count === 0, "sender unread did not settle for the second message");
  await groupReadThrough("g", statsGroup, readSecond.message.id);
  const beforeKick = await readStats("e", statsGroup, readSecond.message.id);
  run.check(
    "read-stats tracks the newer message",
    beforeKick.read_count === 1 && beforeKick.member_count === 2 && beforeKick.all_read === false,
  );
  await run.request("e", "DELETE", `/api/groups/${statsGroup}/members/${run.users.get("f").username}`);
  const afterKick = await readStats("e", statsGroup, readSecond.message.id);
  run.check(
    "kicking a non-reader shrinks the denominator",
    afterKick.read_count === 1 && afterKick.member_count === 1 && afterKick.all_read === true,
  );
  await run.request("g", "POST", `/api/groups/${statsGroup}/leave`);
  await run.expectError("former member cannot read stats", "g", "GET", `${statsPath}?message_id=${otherMessage.message.id}`, undefined, 404, "group_not_found");
  const alone = await readStats("e", statsGroup, readSecond.message.id);
  run.check(
    "alone in the group has no denominator",
    alone.read_count === 0 && alone.member_count === 0 && alone.all_read === false,
  );
}

try { await main(); } catch (error) { console.error(`FAIL ${error.message}`); process.exitCode = 1; } finally { await run.finish(); }
