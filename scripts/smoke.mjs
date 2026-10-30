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
const momentsPath = "/api/moments";
const readStats = (who, id, seq) => run.request(who, "GET", `/api/groups/${id}/read-stats?seq=${seq}`);
const seqsOf = (messages) => messages.map((message) => message.seq);
const contiguous = (seqs, first) => seqs.length > 0
  && seqs.every((seq, index) => seq === first + index);

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

  const friendsMoment = await run.request("a", "POST", momentsPath, {
    body: "朋友圈第一条\n第二行",
    visibility: "friends",
  });
  run.check(
    "friends moment is created with text and visibility",
    friendsMoment.visibility === "friends"
      && friendsMoment.mine === true
      && friendsMoment.body.includes("第二行"),
  );
  const privateMoment = await run.request("a", "POST", momentsPath, {
    body: "仅自己可见",
    visibility: "private",
  });
  const momentsForB = await run.request("b", "GET", `${momentsPath}?limit=20`);
  run.check(
    "friend sees friends moment",
    momentsForB.moments.some((moment) => moment.id === friendsMoment.id),
  );
  run.check(
    "friend does not see private moment",
    !momentsForB.moments.some((moment) => moment.id === privateMoment.id),
  );
  const momentsForD = await run.request("d", "GET", `${momentsPath}?limit=20`);
  run.check(
    "non-friend does not see moment",
    !momentsForD.moments.some((moment) => moment.id === friendsMoment.id),
  );
  await run.expectError(
    "non-friend cannot read moment detail",
    "d", "GET", `${momentsPath}/${friendsMoment.id}`, undefined, 403, "not_friend",
  );
  await run.expectError(
    "friend cannot read a private moment",
    "b", "GET", `${momentsPath}/${privateMoment.id}`, undefined, 403, "not_friend",
  );
  const likeOnce = await run.request("b", "PUT", `${momentsPath}/${friendsMoment.id}/like`);
  const likeTwice = await run.request("b", "PUT", `${momentsPath}/${friendsMoment.id}/like`);
  run.check(
    "moment like is idempotent",
    likeOnce.like_count === 1 && likeTwice.like_count === 1 && likeTwice.liked_by_me === true,
  );
  const momentComment = await run.request("b", "POST", `${momentsPath}/${friendsMoment.id}/comments`, {
    body: "说得好",
  });
  const momentDetail = await run.request("a", "GET", `${momentsPath}/${friendsMoment.id}`);
  run.check(
    "moment detail carries like and comment",
    momentDetail.moment.like_count === 1
      && momentDetail.comments.some((comment) => comment.id === momentComment.id),
  );
  await run.request("b", "DELETE", `${momentsPath}/${friendsMoment.id}/comments/${momentComment.id}`);
  const momentDetailAfterDelete = await run.request("a", "GET", `${momentsPath}/${friendsMoment.id}`);
  run.check(
    "comment delete updates the count",
    !momentDetailAfterDelete.comments.some((comment) => comment.id === momentComment.id)
      && momentDetailAfterDelete.moment.comment_count === 0,
  );
  await run.expectError(
    "friend cannot delete another user's moment",
    "b", "DELETE", `${momentsPath}/${friendsMoment.id}`, undefined, 403, "forbidden",
  );
  const seen = await run.request("b", "POST", `${momentsPath}/seen`, {
    last_seen_id: friendsMoment.id,
  });
  const newerMoment = await run.request("a", "POST", momentsPath, {
    body: "红点新动态",
    visibility: "friends",
  });
  const momentUnread = await run.request("b", "GET", `${momentsPath}/unread`);
  run.check(
    "new friend moment sets the unread dot",
    momentUnread.has_new === true && momentUnread.last_seen_id === seen.last_seen_id,
  );
  await run.request("b", "POST", `${momentsPath}/seen`, { last_seen_id: newerMoment.id });
  const momentUnreadAfter = await run.request("b", "GET", `${momentsPath}/unread`);
  run.check("seen clears the unread dot", momentUnreadAfter.has_new === false);

  for (let index = 0; index < 21; index += 1) {
    await run.request("a", "POST", momentsPath, {
      body: `分页动态 ${index}`,
      visibility: "friends",
    });
  }
  const momentsPageOne = await run.request("b", "GET", `${momentsPath}?limit=20`);
  run.check(
    "moment feed reports more pages",
    momentsPageOne.has_more === true && momentsPageOne.moments.length === 20,
  );
  const momentsPageTwo = await run.request(
    "b", "GET", `${momentsPath}?limit=20&before_id=${momentsPageOne.next_before_id}`,
  );
  const pageOneIDs = new Set(momentsPageOne.moments.map((moment) => moment.id));
  run.check(
    "moment pages do not overlap and move backwards",
    momentsPageTwo.moments.every((moment) => !pageOneIDs.has(moment.id))
      && momentsPageTwo.moments.every((moment) => moment.id < momentsPageOne.next_before_id),
  );
  await run.request("a", "DELETE", `${momentsPath}/${friendsMoment.id}`);
  await run.expectError(
    "deleted moment is gone",
    "b", "GET", `${momentsPath}/${friendsMoment.id}`, undefined, 404, "moment_not_found",
  );

  const directID = randomUUID();
  const aSocket = await run.openSocket("a");
  try {
    const first = await run.request("a", "POST", `${directPath("b")}/messages`, { body: "私聊第一条\n🙂", client_message_id: directID });
    const duplicate = await run.request("a", "POST", `${directPath("b")}/messages`, { body: "私聊第一条\n🙂", client_message_id: directID });
    run.check("private conversation starts at seq 1", first.message.seq === 1);
    run.check(
      "private send retry is idempotent and keeps the seq",
      first.message.id === duplicate.message.id && duplicate.message.seq === first.message.seq,
    );
    const readerB = await run.openSocket("b");
    try {
      const readerItem = directOf(await list("b"), run.users.get("a").username);
      run.check(
        "private conversation exposes seq fields",
        Number.isInteger(readerItem.last_seq) && !("peer_last_read_message_id" in readerItem),
      );
      run.check("private unread is last_seq minus the read seq", readerItem.last_seq === 1 && readerItem.unread_count === 1);
      const ack = await readerB.focusDirect(run.users.get("a").username);
      run.check("direct focus acknowledges last_read_seq", !ack.error && ack.last_read_seq === 1);
      run.check("private unread clears after focus", directOf(await list("b"), run.users.get("a").username).unread_count === 0);
      const peerItem = directOf(await list("a"), run.users.get("b").username);
      run.check(
        "peer read receipt uses the message seq",
        peerItem.peer_last_read_seq === 1 && !("peer_last_read_message_id" in peerItem),
      );

      const pushedFrom = aSocket.events.length;
      const replyKey = randomUUID();
      const second = await run.request("b", "POST", `${directPath("a")}/messages`, { body: "私聊第二条", client_message_id: replyKey });
      const pushed = await aSocket.waitFor(
        (event) => event.type === "message.created" && event.message?.client_message_id === replyKey,
        "the private push never arrived",
        pushedFrom,
      );
      run.check("both directions share one conversation seq", second.message.seq === 2);
      run.check("websocket private pushes carry seq", pushed.message.seq === 2 && pushed.message.id === second.message.id);
      await until(
        async () => directOf(await list("a"), run.users.get("b").username)?.unread_count === 1
          && directOf(await list("a"), run.users.get("b").username)?.last_seq === 2,
        "private unread did not settle on the sender",
      );
      run.check("private unread counts by seq across both directions", true);
    } finally {
      readerB.close();
    }
  } finally {
    aSocket.close();
  }

  await run.expectError("non-friend send denied", "d", "POST", `${directPath("a")}/messages`, { body: "no", client_message_id: randomUUID() }, 403, "not_friends");
  const outsider = await run.request("d", "GET", `${directPath("a")}/messages`);
  run.check("outsider sees only their own empty pair", outsider.messages.length === 0);

  for (let index = 0; index < 32; index += 1) {
    await run.request("a", "POST", `${directPath("b")}/messages`, { body: `private page ${index}`, client_message_id: randomUUID() });
  }
  const newest = await run.request("b", "GET", `${directPath("a")}/messages?limit=30`);
  const older = await run.request("b", "GET", `${directPath("a")}/messages?before_seq=${newest.messages[0].seq}&limit=30`);
  run.check(
    "private history pages by seq without overlap",
    newest.has_more && newest.messages.length === 30 && older.messages.length === 4
      && older.messages.at(-1).seq < newest.messages[0].seq,
  );
  run.check("private history seqs are contiguous", contiguous(seqsOf(newest.messages), 5) && contiguous(seqsOf(older.messages), 1));
  const directAfter = await run.request("b", "GET", `${directPath("a")}/messages?after_seq=1&limit=50`);
  run.check("private reconnect cursor catches up by seq", contiguous(seqsOf(directAfter.messages), 2) && directAfter.messages.length === 33);
  const directGap = await run.request("b", "GET", `${directPath("a")}/messages?after_seq=30&limit=50`);
  run.check("private after_seq backfills a gap", contiguous(seqsOf(directGap.messages), 31) && directGap.messages.length === 4);
  const sync = await run.request("b", "GET", `/api/messages/sync?after_id=${newest.messages[0].id}&limit=200`);
  run.check(
    "private feed sync still uses after_id and carries seq",
    sync.messages.length === 29 && sync.messages.every((message) => Number.isInteger(message.seq) && message.seq >= 1)
      && sync.messages.every((message, index) => index === 0 || message.id > sync.messages[index - 1].id),
  );
  await until(
    async () => directOf(await list("b"), run.users.get("a").username)?.unread_count === 32,
    "private unread did not settle",
  );
  run.check("private unread follows the seq counter", true);

  const concurrent = await Promise.all(Array.from({ length: 10 }, (_, index) =>
    run.request("a", "POST", `${directPath("b")}/messages`, { body: `private concurrent ${index}`, client_message_id: randomUUID() })));
  const concurrentSeqs = concurrent.map((response) => response.message.seq).sort((left, right) => left - right);
  run.check("concurrent private sends reuse no seq and leave no gap", contiguous(concurrentSeqs, 35));
  await until(
    async () => directOf(await list("b"), run.users.get("a").username)?.last_seq === 44
      && directOf(await list("b"), run.users.get("a").username)?.unread_count === 42,
    "private unread did not follow the concurrent seqs",
  );
  run.check("private seq ends at the sent count", true);

  // Bounded and rewinding HTTP reads are gone: focus always advances to the
  // conversation counter, so only the latest position can be recorded.
  await run.expectGone("legacy conversation read endpoint is gone", "b", "POST", `${directPath("a")}/read`);
  await run.expectGone("legacy batch read endpoint is gone", "b", "POST", "/api/read", { reads: [] });

  await run.expectError("group needs members", "a", "POST", "/api/groups", { name: "empty", member_usernames: [] }, 400, "invalid_members");
  await run.expectError("group needs valid name", "a", "POST", "/api/groups", { name: "群".repeat(31), member_usernames: [run.users.get("b").username] }, 400, "invalid_group_name");
  await run.expectError("group invite requires own friendship", "a", "POST", "/api/groups", { name: "nonfriend", member_usernames: [run.users.get("d").username] }, 403, "not_friends");
  const id = await run.group("a", "验证群", ["b"]);
  run.check("member sees group", groupOf(await list("b"), id).group.member_count === 2);
  const messageKey = randomUUID();
  const groupFirst = await sendGroup("a", id, "入群前的历史", messageKey);
  const repeated = await sendGroup("a", id, "入群前的历史", messageKey);
  run.check("group conversation starts at seq 1", groupFirst.message.seq === 1);
  run.check(
    "group retry is idempotent and keeps the seq",
    groupFirst.message.id === repeated.message.id && repeated.message.seq === groupFirst.message.seq,
  );
  // v1.7 advances the sender's own position asynchronously after accept.
  await until(async () => groupOf(await list("a"), id)?.unread_count === 0, "group sender unread did not settle");
  run.check("group sender has no own unread", true);
  const groupPeer = groupOf(await list("b"), id);
  run.check(
    "group unread is last_seq minus the read seq",
    groupPeer.last_seq === 1 && groupPeer.unread_count === 1 && !("peer_last_read_seq" in groupPeer),
  );
  const groupReader = await run.openSocket("b");
  try {
    const ack = await groupReader.focusGroup(id);
    run.check("group focus acknowledges last_read_seq", !ack.error && ack.last_read_seq === 1);
    run.check("group focus clears unread", groupOf(await list("b"), id).unread_count === 0);
  } finally {
    groupReader.close();
  }
  await run.request("b", "POST", `/api/groups/${id}/members`, { usernames: [run.users.get("c").username] });
  run.check("ordinary member can invite their friend", groupOf(await list("c"), id).group.member_count === 3);
  run.check(
    "new member baseline is the current seq counter",
    groupOf(await list("c"), id).unread_count === 0 && groupOf(await list("c"), id).last_seq === 1,
  );
  const oldHistory = await run.request("c", "GET", `/api/groups/${id}/messages`);
  run.check("new member sees prejoin history", oldHistory.messages[0].body === "入群前的历史" && oldHistory.messages[0].seq === 1);
  await run.request("a", "POST", `/api/groups/${id}/members`, { usernames: [run.users.get("c").username] });
  run.check("duplicate invite ignored", groupOf(await list("a"), id).group.member_count === 3);
  await run.expectError("outsider group read denied", "d", "GET", `/api/groups/${id}`, undefined, 404, "group_not_found");
  await run.expectError("ordinary member rename denied", "b", "PATCH", `/api/groups/${id}`, { name: "no" }, 403, "not_owner");
  await run.expectError("ordinary member kick denied", "b", "DELETE", `/api/groups/${id}/members/${run.users.get("c").username}`, undefined, 403, "not_owner");
  await run.request("a", "PATCH", `/api/groups/${id}`, { name: "已改名的验证群" });
  run.check("rename persisted for all members", groupOf(await list("b"), id).group.name === "已改名的验证群");
  for (let index = 0; index < 35; index += 1) await sendGroup("a", id, `group page ${index}`);
  const groupPage = await run.request("b", "GET", `/api/groups/${id}/messages?limit=30`);
  const groupOlder = await run.request("b", "GET", `/api/groups/${id}/messages?limit=30&before_seq=${groupPage.messages[0].seq}`);
  run.check(
    "group history pages by seq without overlap",
    groupPage.has_more && groupPage.messages.length === 30 && groupOlder.messages.length === 6
      && groupOlder.messages.at(-1).seq < groupPage.messages[0].seq,
  );
  run.check("group history seqs are contiguous", contiguous(seqsOf(groupPage.messages), 7) && contiguous(seqsOf(groupOlder.messages), 1));
  const groupAfter = await run.request("b", "GET", `/api/groups/${id}/messages?limit=50&after_seq=1`);
  run.check("group reconnect cursor catches up by seq", contiguous(seqsOf(groupAfter.messages), 2) && groupAfter.messages.length === 35);
  const groupBootstrap = await run.request("b", "POST", "/api/bootstrap", {
    after_id: 0,
    groups: [{ group_id: id, after_seq: 5 }],
    limit: 200,
  });
  const groupDelta = groupBootstrap.groups.find((item) => item.group_id === id);
  run.check(
    "bootstrap group cursor is after_seq in seq units",
    groupDelta?.cursor === 36 && contiguous(seqsOf(groupDelta.messages), 6),
  );
  await until(
    async () => groupOf(await list("b"), id).unread_count === 35,
    "group unread did not follow the seq counter",
  );
  run.check("group unread follows the seq counter", true);
  const groupConcurrent = await Promise.all(Array.from({ length: 12 }, (_, index) => sendGroup("a", id, `group concurrent ${index}`)));
  const groupConcurrentSeqs = groupConcurrent.map((response) => response.message.seq).sort((left, right) => left - right);
  run.check("concurrent group sends reuse no seq and leave no gap", contiguous(groupConcurrentSeqs, 37));
  const groupGap = await run.request("b", "GET", `/api/groups/${id}/messages?limit=50&after_seq=36`);
  run.check("group after_seq backfills a gap", contiguous(seqsOf(groupGap.messages), 37) && groupGap.messages.length === 12);
  await run.expectGone("legacy group read endpoint is gone", "b", "POST", `/api/groups/${id}/read`);
  await run.expectError("conflicting group cursors rejected", "a", "GET", `/api/groups/${id}/messages?before_seq=10&after_seq=1`, undefined, 400, "invalid_request");
  await run.request("a", "DELETE", `/api/groups/${id}/members/${run.users.get("c").username}`);
  await run.expectError("kicked member loses history", "c", "GET", `/api/groups/${id}/messages`, undefined, 404, "group_not_found");
  await run.request("a", "POST", `/api/groups/${id}/members`, { usernames: [run.users.get("c").username] });
  run.check(
    "reinvited member baseline advances to the counter",
    groupOf(await list("c"), id).unread_count === 0 && groupOf(await list("c"), id).last_seq === 48,
  );
  await run.request("c", "POST", `/api/groups/${id}/leave`);
  run.check("member can leave", !groupOf(await list("c"), id));
  await run.request("a", "DELETE", `/api/friends/${run.users.get("b").username}`);
  await run.expectError("deleted friend private send denied", "a", "POST", `${directPath("b")}/messages`, { body: "no", client_message_id: randomUUID() }, 403, "not_friends");
  run.check("deleted friend private history retained", (await run.request("a", "GET", `${directPath("b")}/messages`)).messages.length > 0);
  const afterUnfriend = await sendGroup("b", id, "删好友仍可群聊");
  run.check("group seq keeps counting after unfriending", afterUnfriend.message.seq === 49);
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
  run.check("group unread matches for peers", peerF.unread_count === 1 && peerG.unread_count === 1);
  run.check(
    "bootstrap group unread matches conversations",
    groupOf(await bootstrapOf("f"), statsGroup).unread_count === peerF.unread_count,
  );
  run.check(
    "group sender has no own unread in bootstrap",
    groupOf(await bootstrapOf("e"), statsGroup).unread_count === 0,
  );
  const ownStats = await readStats("e", statsGroup, readFirst.message.seq);
  run.check(
    "read-stats takes seq and returns the global message id",
    ownStats.seq === 1 && ownStats.message_id === readFirst.message.id
      && ownStats.read_count === 0 && ownStats.member_count === 2 && ownStats.all_read === false,
  );
  await run.expectError("read-stats needs a seq", "e", "GET", statsPath, undefined, 400, "invalid_request");
  await run.expectError("read-stats rejects zero seq", "e", "GET", `${statsPath}?seq=0`, undefined, 400, "invalid_request");
  await run.expectError("read-stats rejects malformed seq", "e", "GET", `${statsPath}?seq=abc`, undefined, 400, "invalid_request");
  await run.expectError("outsider cannot read stats or probe messages", "d", "GET", `${statsPath}?seq=1`, undefined, 404, "group_not_found");

  const readerF = await run.openSocket("f");
  const readerG = await run.openSocket("g");
  try {
    run.check("reader f focuses the group", !(await readerF.focusGroup(statsGroup)).error);
    await until(async () => (await readStats("e", statsGroup, 1)).read_count === 1, "the focused reader did not read");
    run.check("read-stats counts a partial reader", true);
    run.check("reader g focuses the group", !(await readerG.focusGroup(statsGroup)).error);
    await until(async () => (await readStats("e", statsGroup, 1)).all_read, "the second reader did not read");
    run.check(
      "read-stats reports all read",
      (await readStats("e", statsGroup, 1)).read_count === 2 && (await readStats("e", statsGroup, 1)).member_count === 2,
    );
    // Implicit read resolves focus at flush time (100ms after enqueue), so
    // blurring after the send races the advance. Clear f's focus first.
    readerF.blur();
    await delay(150);
    const otherMessage = await sendGroup("g", statsGroup, "他人的消息");
    run.check("group seqs are contiguous in the stats group", otherMessage.message.seq === 2);
    await run.expectError(
      "read-stats rejects another member's message",
      "e",
      "GET",
      `${statsPath}?seq=${otherMessage.message.seq}`,
      undefined,
      403,
      "not_your_message",
    );
    const readSecond = await sendGroup("e", statsGroup, "已读第二条");
    run.check("group seq reaches three", readSecond.message.seq === 3);
    run.check("reader g refocuses the group", !(await readerG.focusGroup(statsGroup)).error);
    await until(async () => (await readStats("e", statsGroup, 3)).read_count === 1, "the focused reader did not catch up");
    const beforeKick = await readStats("e", statsGroup, readSecond.message.seq);
    run.check(
      "read-stats tracks the newer seq",
      beforeKick.seq === 3 && beforeKick.message_id === readSecond.message.id
        && beforeKick.read_count === 1 && beforeKick.member_count === 2 && beforeKick.all_read === false,
    );
    await until(async () => groupOf(await list("e"), statsGroup)?.unread_count === 0, "sender unread did not settle for the second message");
    run.check(
      "blurred reader keeps unread for unread seqs",
      groupOf(await list("f"), statsGroup).unread_count === 2
        && groupOf(await list("g"), statsGroup).unread_count === 0,
    );
    await run.request("e", "DELETE", `/api/groups/${statsGroup}/members/${run.users.get("f").username}`);
    const afterKick = await readStats("e", statsGroup, readSecond.message.seq);
    run.check(
      "kicking a non-reader shrinks the denominator",
      afterKick.read_count === 1 && afterKick.member_count === 1 && afterKick.all_read === true,
    );
  } finally {
    readerF.close();
    readerG.close();
  }
  await run.request("g", "POST", `/api/groups/${statsGroup}/leave`);
  await run.expectError("former member cannot read stats", "g", "GET", `${statsPath}?seq=2`, undefined, 404, "group_not_found");
  const alone = await readStats("e", statsGroup, 3);
  run.check(
    "alone in the group has no denominator",
    alone.read_count === 0 && alone.member_count === 0 && alone.all_read === false,
  );

  await implicitReadOverWebSocket();
}

async function implicitReadOverWebSocket() {
  for (const key of ["h", "i", "j"]) await run.register(key);
  await run.befriend("h", "i");
  await run.befriend("h", "j");
  const sockets = [];
  const open = async (key) => {
    const socket = await run.openSocket(key);
    sockets.push(socket);
    return socket;
  };
  try {
    const live = await run.group("h", "隐式已读群", ["i", "j"]);
    const owner = await open("h");
    const readerI = await open("i");
    const readerJ = await open("j");
    run.check("viewers focus the group", !(await readerI.focusGroup(live)).error && !(await readerJ.focusGroup(live)).error);

    const ownerFrom = owner.events.length;
    const first = await sendGroup("h", live, "隐式已读群消息");
    run.check("the first group message gets seq 1", first.message.seq === 1);
    await owner.waitFor((event) => event.type === "group.read" && event.group_id === live, "the sender never received group.read", ownerFrom);
    run.check("sender receives group.read when focused viewers read", true);
    await until(async () => (await readStats("h", live, 1)).all_read, "focused viewers did not complete the receipt");
    const stats = await readStats("h", live, 1);
    run.check(
      "focused viewers implicitly read the seq",
      stats.seq === 1 && stats.message_id === first.message.id && stats.read_count === 2 && stats.member_count === 2,
    );
    run.check(
      "no member has unread after implicit read",
      (await Promise.all(["h", "i", "j"].map((key) => list(key)))).every((conversations) => groupOf(conversations, live).unread_count === 0),
    );

    readerJ.blur();
    await delay(150);
    const second = await sendGroup("h", live, "j 已离开");
    run.check("the second group message gets seq 2", second.message.seq === 2);
    await until(async () => (await readStats("h", live, 2)).read_count === 1, "the focused viewer did not read");
    await delay(400);
    run.check(
      "a blurred viewer is not auto-read",
      (await readStats("h", live, 2)).read_count === 1 && groupOf(await list("j"), live).unread_count === 1,
    );
    const ack = await readerJ.focusGroup(live);
    await until(async () => (await readStats("h", live, 2)).all_read, "refocus did not catch up");
    run.check(
      "refocus catches up the missed seq",
      !ack.error && ack.last_read_seq === 2 && groupOf(await list("j"), live).unread_count === 0,
    );

    const outsider = await open("a");
    run.check("a non-member cannot focus the group", (await outsider.focusGroup(live)).error === "not_group_member");

    const owner2 = owner.events.length;
    const directAck = await readerI.focusDirect(run.users.get("h").username);
    const direct = await run.request("h", "POST", `${directPath("i")}/messages`, { body: "私聊隐式已读", client_message_id: randomUUID() });
    run.check("the first private message gets seq 1", direct.message.seq === 1);
    const readEvent = await owner.waitFor(
      (event) => event.type === "messages.read" && event.last_read_seq >= direct.message.seq,
      "the sender never received messages.read",
      owner2,
    );
    run.check(
      "private focus reads without any read request",
      !directAck.error && directAck.last_read_seq === 0 && readEvent.reader === run.users.get("i").username,
    );
    run.check(
      "private receipt and unread settle by seq",
      directOf(await list("h"), run.users.get("i").username).peer_last_read_seq >= direct.message.seq
        && directOf(await list("i"), run.users.get("h").username).unread_count === 0,
    );
    const refused = readerI.events.length;
    readerI.send({ type: "focus", kind: "direct", username: run.users.get("a").username });
    const denied = await readerI.waitFor((event) => event.type === "focus.ack" && event.kind === "direct" && event.username === run.users.get("a").username, "non-friend focus got no answer", refused);
    run.check("a non-friend cannot be focused", denied.error === "not_friend");
  } finally {
    for (const socket of sockets) socket.close();
  }
}

try { await main(); } catch (error) { console.error(`FAIL ${error.message}`); process.exitCode = 1; } finally { await run.finish(); }
