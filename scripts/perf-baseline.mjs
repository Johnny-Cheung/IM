#!/usr/bin/env node
import { randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { delay, TestRun } from "./lib/test-client.mjs";

const WARMUP_ROUNDS = 3;
const SAMPLE_ROUNDS = 20;
const run = new TestRun("perf-baseline");
const samples = new Map();

const directPath = (who) => `/api/conversations/${run.users.get(who).username}`;
const listConversations = (who) => run.request(who, "GET", "/api/conversations");
const directConversation = (list, username) =>
  list.conversations.find((item) => item.type === "direct" && item.user.username === username);
const groupConversation = (list, groupID) =>
  list.conversations.find((item) => item.type === "group" && item.group.id === groupID);
const sendDirect = (who, peer, body) => run.request(
  who,
  "POST",
  `${directPath(peer)}/messages`,
  { body, client_message_id: randomUUID() },
);
const sendGroup = (who, groupID, body) => run.request(
  who,
  "POST",
  `/api/groups/${groupID}/messages`,
  { body, client_message_id: randomUUID() },
);

async function measure(name, enabled, operation) {
  const started = performance.now();
  const result = await operation();
  if (enabled) {
    const values = samples.get(name) ?? [];
    values.push(performance.now() - started);
    samples.set(name, values);
  }
  return result;
}

function percentile(values, percentage) {
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.max(0, Math.ceil(sorted.length * percentage) - 1);
  return sorted[index];
}

function summarize(values) {
  return {
    n: values.length,
    p50_ms: Number(percentile(values, 0.5).toFixed(2)),
    p95_ms: Number(percentile(values, 0.95).toFixed(2)),
    max_ms: Number(Math.max(...values).toFixed(2)),
  };
}

async function prepare() {
  for (const key of ["a", "b", "c"]) await run.register(key);
  await run.befriend("a", "b");
  await run.befriend("a", "c");
  const groupID = await run.group("a", "基线群", ["b", "c"]);

  for (let index = 0; index < 31; index += 1) {
    await sendDirect("a", "b", `baseline private history ${index}`);
    await sendGroup("a", groupID, `baseline group history ${index}`);
  }
  const reader = await run.openSocket("b");
  await reader.focusDirect(run.users.get("a").username);
  await reader.focusGroup(groupID);
  return { groupID, reader };
}

async function verifyUnread(groupID, reader) {
  // Blur first so neither message is auto-read before the unread check.
  reader.blur();
  await delay(150);
  await sendDirect("a", "b", "baseline private verify");
  await sendGroup("a", groupID, "baseline group verify");
  const list = await listConversations("b");
  run.check(
    "private unread visible",
    (directConversation(list, run.users.get("a").username)?.unread_count ?? 0) >= 1,
  );
  run.check("group unread visible", (groupConversation(list, groupID)?.unread_count ?? 0) >= 1);
  await reader.focusDirect(run.users.get("a").username);
  await reader.focusGroup(groupID);
}

async function sampleRound(groupID, enabled, reader) {
  await measure(
    "direct_send",
    enabled,
    () => sendDirect("a", "b", "baseline private sample"),
  );
  await measure(
    "group_send",
    enabled,
    () => sendGroup("a", groupID, "baseline group sample"),
  );

  await measure("conversations", enabled, () => listConversations("b"));

  await measure(
    "direct_history",
    enabled,
    () => run.request("b", "GET", `${directPath("a")}/messages?limit=30`),
  );
  await measure(
    "group_history",
    enabled,
    () => run.request("b", "GET", `/api/groups/${groupID}/messages?limit=30`),
  );
  await measure(
    "direct_focus",
    enabled,
    () => reader.focusDirect(run.users.get("a").username),
  );
  await measure(
    "group_focus",
    enabled,
    () => reader.focusGroup(groupID),
  );
}

async function writeReport() {
  const metrics = Object.fromEntries(
    [...samples.entries()].map(([name, values]) => [name, summarize(values)]),
  );
  const report = {
    run_id: run.id,
    base: run.base,
    warmup: WARMUP_ROUNDS,
    samples: SAMPLE_ROUNDS,
    metrics,
  };
  await writeFile(join(run.reportDir, "perf-baseline.json"), `${JSON.stringify(report, null, 2)}\n`, { mode: 0o600 });
  return report;
}

async function main() {
  console.log(`API baseline ${run.id}: ${run.base}`);
  for (const path of ["/health", "/ready"]) {
    const response = await fetch(run.base + path, { signal: AbortSignal.timeout(10000) });
    run.check(`${path} is healthy`, response.status === 200 && (await response.json()).status === "ok");
  }

  const { groupID, reader } = await prepare();
  try {
    await verifyUnread(groupID, reader);
    for (let index = 0; index < WARMUP_ROUNDS; index += 1) await sampleRound(groupID, false, reader);
    for (let index = 0; index < SAMPLE_ROUNDS; index += 1) await sampleRound(groupID, true, reader);
  } finally {
    reader.close();
  }

  const report = await writeReport();
  for (const [name, metric] of Object.entries(report.metrics)) {
    console.log(`${name.padEnd(18)} n=${metric.n} p50=${metric.p50_ms}ms p95=${metric.p95_ms}ms max=${metric.max_ms}ms`);
  }
  console.log(`baseline report: ${join(run.reportDir, "perf-baseline.json")}`);
}

try {
  await main();
} catch (error) {
  console.error(`FAIL ${error.message}`);
  process.exitCode = 1;
} finally {
  await run.finish();
}
