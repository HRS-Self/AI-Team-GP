import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, readdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, dirname } from "node:path";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { loadProjectPaths } from "../src/paths/project-paths.js";
import { approveMergeApproval } from "../src/lane_b/gates/merge-approval.js";
import { validateKnowledgeChangeEvent } from "../src/contracts/validators/index.js";

function writeJson(absPath, obj) {
  mkdirSync(dirname(resolve(absPath)), { recursive: true });
  writeFileSync(absPath, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

test("Lane B emits a knowledge change event on merge approval (merge signal)", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-lane-b-events-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "t", projectMode: "greenfield", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });

  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  const wid = "W-merge-1";
  const workDirAbs = join(paths.laneB.workAbs, wid);
  mkdirSync(join(workDirAbs, "CI"), { recursive: true });

  writeJson(join(workDirAbs, "META.json"), { version: 1, workId: wid });
  writeJson(join(workDirAbs, "PR.json"), {
    version: 1,
    workId: wid,
    owner: "o",
    repo: "r",
    pr_number: 123,
    url: "https://example.invalid/pr/123",
    base_branch: "main",
    head_branch: `ai/${wid}/repo-a/branch`,
  });
  writeJson(join(workDirAbs, "MERGE_APPROVAL.json"), {
    version: 1,
    workId: wid,
    status: "pending",
    mode: "manual",
    bundle_hash: "bundle_x",
    approved_at: null,
    approved_by: null,
    reason_codes: [],
    notes: null,
  });
  writeJson(join(workDirAbs, "CI", "CI_Status.json"), {
    version: 1,
    workId: wid,
    pr_number: 123,
    head_sha: "b".repeat(40),
    captured_at: "2026-02-09T00:00:00.000Z",
    overall: "success",
    checks: [],
    latest_feedback: null,
  });

  // QA audit prerequisites (merge approval is an audit of obligations met).
  writeJson(join(workDirAbs, "BUNDLE.json"), {
    version: 1,
    work_id: wid,
    bundle_hash: "bundle_x",
    repos: [],
  });
  writeJson(join(workDirAbs, "QA", "obligations.json"), {
    version: 1,
    workId: wid,
    created_at: "2026-02-09T00:00:00.000Z",
    risk_level: "low",
    changed_paths_by_repo: [],
    must_add_unit: false,
    must_add_integration: false,
    must_add_e2e: false,
    suggested_test_directives: [],
    api_surface_bindings_by_repo: [],
  });

  const res = await approveMergeApproval({ workId: wid, approvedBy: "human", notes: null });
  assert.equal(res.ok, true);

  const eventsSegDir = paths.laneA.eventsSegmentsAbs;
  assert.equal(existsSync(eventsSegDir), true);
  const segFiles = readdirSync(eventsSegDir)
    .filter((f) => String(f).endsWith(".jsonl"))
    .sort((a, b) => String(a).localeCompare(String(b)));
  assert.ok(segFiles.length >= 1);
  const newest = segFiles.at(-1);
  const text = readFileSync(join(eventsSegDir, newest), "utf8");
  const lines = String(text || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  assert.ok(lines.length >= 1);
  const last = JSON.parse(lines.at(-1));
  validateKnowledgeChangeEvent(last);
  assert.equal(last.type, "merge");
  assert.equal(last.scope, "repo:repo-a");
  assert.equal(last.commit, "b".repeat(40));
});
