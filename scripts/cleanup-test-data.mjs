#!/usr/bin/env node
// Generate exact-account cleanup SQL from successful-registration manifests.
// Execution remains a separate, explicit operation on the development host.
import { readFile, writeFile } from "node:fs/promises";

const [database, output, ...manifests] = process.argv.slice(2);
if (!["im_v07_verify", "im"].includes(database) || !output || manifests.length === 0) {
  console.error("Usage: node scripts/cleanup-test-data.mjs <im_v07_verify|im> <output.sql> <manifest.json>...");
  process.exit(2);
}
const expectedPort = database === "im" ? "8198" : "8199";
const names = new Set();
for (const path of manifests) {
  const manifest = JSON.parse(await readFile(path, "utf8"));
  if (!/^[a-f0-9]{8}$/.test(manifest.run_id) || new URL(manifest.base).port !== expectedPort
    || !Array.isArray(manifest.usernames)) {
    throw new Error(`Invalid manifest or database/port mismatch: ${path}`);
  }
  for (const name of manifest.usernames) {
    if (typeof name !== "string" || !new RegExp(`^v7_${manifest.run_id}_[a-z0-9]{1,4}$`).test(name)) {
      throw new Error(`Unexpected account in manifest: ${path}`);
    }
    names.add(name);
  }
}
if (names.size === 0) throw new Error("No registered accounts to clean up");
const values = [...names].sort().map((name) => `0x${Buffer.from(name).toString("hex")}`).join(",");
const table = `\`${database}\`.\`users\``;
const sql = `-- Exact synthetic accounts from ${manifests.length} manifests; no wildcard deletion.\n`
  + `SELECT COUNT(*) AS matching_test_accounts FROM ${table} WHERE username IN (${values});\n`
  + `DELETE FROM ${table} WHERE username IN (${values});\nSELECT ROW_COUNT() AS deleted_test_accounts;\n`;
await writeFile(output, sql, { flag: "wx", mode: 0o600 });
console.log(`Prepared cleanup for ${names.size} exact test accounts in ${database}: ${output}`);
