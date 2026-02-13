import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Orchestrator } from "../src/lane_b/orchestrator-lane-b.js";

test("Legacy APPROVAL.* is migrated to PLAN_APPROVAL.* (preserve status)", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-plan-approval-migrate-"));
  const opsRoot = join(root, "ops");
  mkdirSync(join(opsRoot, "ai", "lane_b", "work", "W-1"), { recursive: true });

  const prev = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRoot;
  t.after(() => {
    if (typeof prev === "string") process.env.AI_PROJECT_ROOT = prev;
    else delete process.env.AI_PROJECT_ROOT;
  });

  const workDir = join(opsRoot, "ai", "lane_b", "work", "W-1");
  writeFileSync(join(workDir, "APPROVAL.json"), JSON.stringify({ workId: "W-1", status: "approved" }, null, 2) + "\n", "utf8");
  writeFileSync(join(workDir, "APPROVAL.md"), "# APPROVAL\n", "utf8");

  const orchestrator = new Orchestrator({ repoRoot: process.cwd(), projectRoot: opsRoot });
  const res = await orchestrator.approvalStatus({ workId: "W-1" });
  assert.equal(res.ok, true);

  assert.equal(existsSync(join(workDir, "PLAN_APPROVAL.json")), true);
  assert.equal(existsSync(join(workDir, "PLAN_APPROVAL.md")), true);
  assert.equal(existsSync(join(workDir, "APPROVAL.json")), false);
  assert.equal(existsSync(join(workDir, "APPROVAL.md")), false);
});

