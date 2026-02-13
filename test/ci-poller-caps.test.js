import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, mkdirSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { capCiFeedbackFiles, capCiStatusHistory } from "../src/lane_b/ci/ci-poller.js";

test("ci poller caps: keeps only last N feedback pairs and last N status_history entries", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-ci-caps-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");

  const ciDir = join(root, "ops", "ai", "lane_b", "work", "W-1", "CI");
  mkdirSync(ciDir, { recursive: true });

  // Create 12 feedback pairs with stable names.
  for (let i = 1; i <= 12; i += 1) {
    const ts = `20260205_0000${String(i).padStart(2, "0")}000`;
    const base = `feedback_${ts}`;
    writeFileSync(join(ciDir, `${base}.json`), JSON.stringify({ i }, null, 2) + "\n", "utf8");
    writeFileSync(join(ciDir, `${base}.md`), `# ${i}\n`, "utf8");
  }

  await capCiFeedbackFiles({ ciDir: `ai/lane_b/work/W-1/CI`, maxPairs: 10 });
  const leftJson = readdirSync(ciDir).filter((n) => n.endsWith(".json") && n.startsWith("feedback_")).sort();
  const leftMd = readdirSync(ciDir).filter((n) => n.endsWith(".md") && n.startsWith("feedback_")).sort();
  assert.equal(leftJson.length, 10);
  assert.equal(leftMd.length, 10);
  assert.ok(leftJson[0].includes("_000003"), "oldest two feedback pairs should be removed (kept start at 3)");

  const historyPathAbs = join(ciDir, "CI_Status_History.json");
  writeFileSync(historyPathAbs, JSON.stringify(Array.from({ length: 60 }, (_, idx) => ({ n: idx + 1 })), null, 2) + "\n", "utf8");
  await capCiStatusHistory({ historyPath: `ai/lane_b/work/W-1/CI/CI_Status_History.json`, maxEntries: 50 });
  const capped = JSON.parse(readFileSync(historyPathAbs, "utf8"));
  assert.equal(capped.length, 50);
  assert.equal(capped[0].n, 11);
  assert.equal(capped[49].n, 60);
});
