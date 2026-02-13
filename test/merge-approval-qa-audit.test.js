import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { requestMergeApproval } from "../src/lane_b/gates/merge-approval.js";
import { updateWorkStatus } from "../src/utils/status-writer.js";

function writeJson(absPath, obj) {
  writeFileSync(absPath, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

test("Merge approval is blocked when QA obligations require tests but patch plans have no test edits", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-merge-qa-audit-"));
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = join(root, "ops");
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  const wid = "W-1";
  const repoId = "repo-a";
  const workDirAbs = join(root, "ops", "ai", "lane_b", "work", wid);
  mkdirSync(join(workDirAbs, "CI"), { recursive: true });
  mkdirSync(join(workDirAbs, "patch-plans"), { recursive: true });
  mkdirSync(join(workDirAbs, "QA"), { recursive: true });

  await updateWorkStatus({ workId: wid, stage: "CI_GREEN", blocked: false });
  writeJson(join(workDirAbs, "META.json"), { version: 1, work_id: wid });
  writeJson(join(workDirAbs, "PR.json"), { version: 1, workId: wid, owner: "o", repo: "r", pr_number: 1, url: "u", head_branch: "ai/W-1/repo-a", base_branch: "develop", created_at: "2026-02-08T00:00:00.000Z" });
  writeJson(join(workDirAbs, "CI", "CI_Status.json"), {
    version: 1,
    workId: wid,
    pr_number: 1,
    head_sha: "a".repeat(40),
    captured_at: new Date().toISOString(),
    overall: "success",
    checks: [{ name: "ci", status: "completed", conclusion: "success", url: null, required: null }],
    latest_feedback: null,
  });

  const patchPlanRel = `ai/lane_b/work/${wid}/patch-plans/${repoId}.json`;
  writeJson(join(workDirAbs, "patch-plans", `${repoId}.json`), {
    version: 1,
    work_id: wid,
    repo_id: repoId,
    repo_path: repoId,
    target_branch: { name: "develop", source: "routing", confidence: 1 },
    team_id: "Tooling",
    kind: "Service",
    is_hexa: false,
    derived_from: { proposal_id: `ai/lane_b/work/${wid}/proposals/${repoId}.json`, proposal_hash: "abc", proposal_agent_id: "Tooling__planner__01", timestamp: "2026-02-03T00:00:00.000Z" },
    intent_summary: "Change code without tests",
    scope: { allowed_paths: ["src/**"], forbidden_paths: [], allowed_ops: ["edit"] },
    edits: [{ path: "src/index.js", op: "edit", rationale: "Change code", patch: "--- a/src/index.js\n+++ b/src/index.js\n@@ -1 +1 @@\n-old\n+new\n" }],
    commands: { cwd: ".", package_manager: null, install: null, lint: null, test: null, build: null },
    risk: { level: "normal", notes: "" },
    constraints: { no_branch_create: true, requires_training: false, hexa_authoring_mode: null, blockly_compat_required: null },
  });

  writeJson(join(workDirAbs, "BUNDLE.json"), {
    version: 1,
    work_id: wid,
    bundle_hash: "deadbeef",
    repos: [{ repo_id: repoId, patch_plan_json_path: patchPlanRel }],
  });

  writeJson(join(workDirAbs, "QA", "obligations.json"), {
    version: 1,
    workId: wid,
    created_at: new Date().toISOString(),
    risk_level: "normal",
    changed_paths_by_repo: [{ repo_id: repoId, paths: ["src/index.js"] }],
    must_add_unit: true,
    must_add_integration: false,
    must_add_e2e: false,
    suggested_test_directives: [],
    api_surface_bindings_by_repo: [],
  });

  const res = await requestMergeApproval({ workId: wid, dryRun: true });
  assert.equal(res.ok, false);
  assert.ok(String(res.message || "").toLowerCase().includes("qa obligations"));
});

