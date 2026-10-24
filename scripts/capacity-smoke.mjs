#!/usr/bin/env node
// Membership boundary check, using independent accounts in the verification DB.
import { TestRun } from "./lib/test-client.mjs";

const run = new TestRun("capacity-smoke");
async function main() {
  await run.register("o");
  const members = [];
  for (let index = 0; index < 100; index += 1) {
    const key = `m${index}`;
    await run.register(key);
    await run.befriend("o", key);
    members.push(key);
  }
  const full = await run.group("o", "100 人上限验证", members.slice(0, 99));
  const fullInfo = await run.request("o", "GET", `/api/groups/${full}`);
  run.check("owner included in 100 member limit", fullInfo.group.member_count === 100);
  await run.request("o", "POST", `/api/groups/${full}/members`, { usernames: [run.users.get(members[0]).username] });
  run.check("existing invite in full group is ignored", (await run.request("o", "GET", `/api/groups/${full}`)).group.member_count === 100);
  await run.expectError("101st member rejected", "o", "POST", `/api/groups/${full}/members`, { usernames: [run.users.get(members[99]).username] }, 409, "group_full");
  await run.expectError("oversized group creation rejected", "o", "POST", "/api/groups", {
    name: "超员建群",
    member_usernames: members.map((key) => run.users.get(key).username),
  }, 409, "group_full");
  const nearFull = await run.group("o", "并发邀请边界验证", members.slice(0, 98));
  const results = await Promise.allSettled(members.slice(98).map((key) =>
    run.request("o", "POST", `/api/groups/${nearFull}/members`, { usernames: [run.users.get(key).username] })));
  run.check("only one competing invite succeeds", results.filter((item) => item.status === "fulfilled").length === 1);
  const failure = results.find((item) => item.status === "rejected");
  run.check("other competing invite rejected at capacity", failure?.reason.status === 409 && failure.reason.code === "group_full");
  run.check("concurrent invites never exceed 100", (await run.request("o", "GET", `/api/groups/${nearFull}`)).group.member_count === 100);
}
try { await main(); } catch (error) { console.error(`FAIL ${error.message}`); process.exitCode = 1; } finally { await run.finish(); }
