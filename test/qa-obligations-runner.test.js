import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runQaObligations } from "../src/lane_b/qa/qa-obligations-runner.js";

function writeJson(absPath, obj) {
  writeFileSync(absPath, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function basePatchPlan({ workId, repoId } = {}) {
  const wid = String(workId || "W-1");
  const rid = String(repoId || "repo-a");
  return {
    version: 1,
    work_id: wid,
    repo_id: rid,
    repo_path: rid,
    target_branch: { name: "develop", source: "routing", confidence: 1 },
    team_id: "Tooling",
    kind: "Service",
    is_hexa: false,
    derived_from: { proposal_id: `ai/lane_b/work/${wid}/proposals/${rid}.json`, proposal_hash: "abc", proposal_agent_id: "Tooling__planner__01", timestamp: "2026-02-03T00:00:00.000Z" },
    intent_summary: "Test plan",
    scope: { allowed_paths: ["src/**", "openapi.yaml", "test/**"], forbidden_paths: [], allowed_ops: ["edit", "add"] },
    edits: [],
    commands: { cwd: ".", package_manager: null, install: null, lint: null, test: null, build: null },
    risk: { level: "normal", notes: "" },
    constraints: { no_branch_create: true, requires_training: false, hexa_authoring_mode: null, blockly_compat_required: null },
  };
}

test("qa-obligations emits unit+integration requirements from changed paths and repo_index bindings", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-qa-obligations-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "t", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const opsRootAbs = knowledgeRepo.opsRootAbs;
  const knowledgeRootAbs = knowledgeRepo.knowledgeRootAbs;

  const workId = "W-1";
  const repoId = "repo-a";
  const workDirAbs = join(opsRootAbs, "ai", "lane_b", "work", workId);
  mkdirSync(join(workDirAbs, "patch-plans"), { recursive: true });

  const patchPlanRel = `ai/lane_b/work/${workId}/patch-plans/${repoId}.json`;
  const patchPlanAbs = join(opsRootAbs, patchPlanRel);
  const plan = basePatchPlan({ workId, repoId });
  plan.edits.push({
    path: "src/index.js",
    op: "edit",
    rationale: "Change code",
    patch: "--- a/src/index.js\n+++ b/src/index.js\n@@ -1 +1 @@\n-old\n+new\n",
  });
  plan.edits.push({
    path: "openapi.yaml",
    op: "edit",
    rationale: "Change contract",
    patch: "--- a/openapi.yaml\n+++ b/openapi.yaml\n@@ -1 +1 @@\n-openapi: 3.0.0\n+openapi: 3.0.1\n",
  });
  writeJson(patchPlanAbs, plan);

  writeJson(join(workDirAbs, "BUNDLE.json"), {
    version: 1,
    work_id: workId,
    bundle_hash: "deadbeef",
    repos: [{ repo_id: repoId, patch_plan_json_path: patchPlanRel }],
  });

  // Minimal repo_index to provide api_surface bindings.
  const idxDir = join(knowledgeRootAbs, "evidence", "index", "repos", repoId);
  mkdirSync(idxDir, { recursive: true });
  writeJson(join(idxDir, "repo_index.json"), {
    version: 1,
    repo_id: repoId,
    scanned_at: "2026-02-08T00:00:00.000Z",
    head_sha: "a".repeat(40),
    languages: [],
    entrypoints: ["README.md"],
    build_commands: { package_manager: "npm", install: [], lint: [], build: [], test: [], scripts: {}, evidence_files: [] },
    hotspots: [],
    api_surface: { openapi_files: ["openapi.yaml"], routes_controllers: [], events_topics: [] },
    migrations_schema: [],
    cross_repo_dependencies: [],
    fingerprints: { "README.md": { sha256: "a".repeat(64) } },
    dependencies: { version: 1, detected_at: "2026-02-08T00:00:00.000Z", mode: "detected", depends_on: [] },
  });

  const res = await runQaObligations({ projectRoot: opsRootAbs, workId, dryRun: false });
  assert.equal(res.ok, true);
  assert.equal(res.obligations.must_add_unit, true);
  assert.equal(res.obligations.must_add_integration, true);
  assert.equal(res.obligations.must_add_e2e, false);
  assert.ok(Array.isArray(res.obligations.changed_paths_by_repo) && res.obligations.changed_paths_by_repo.length === 1);
  assert.ok(res.obligations.changed_paths_by_repo[0].paths.includes("src/index.js"));
  assert.ok(res.obligations.changed_paths_by_repo[0].paths.includes("openapi.yaml"));
  assert.ok(existsSync(join(opsRootAbs, res.obligations_json)));
  assert.ok(existsSync(join(opsRootAbs, res.obligations_md)));
});

test("qa-obligations loads knowledge QA invariants and elevates obligations when keywords match", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-qa-obligations-inv-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "t", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const opsRootAbs = knowledgeRepo.opsRootAbs;
  const knowledgeRootAbs = knowledgeRepo.knowledgeRootAbs;

  const workId = "W-2";
  const repoId = "repo-a";
  const workDirAbs = join(opsRootAbs, "ai", "lane_b", "work", workId);
  mkdirSync(join(workDirAbs, "patch-plans"), { recursive: true });

  const patchPlanRel = `ai/lane_b/work/${workId}/patch-plans/${repoId}.json`;
  const patchPlanAbs = join(opsRootAbs, patchPlanRel);
  const plan = basePatchPlan({ workId, repoId });
  plan.edits.push({
    path: "src/auth/login.js",
    op: "edit",
    rationale: "Change auth logic",
    patch: "--- a/src/auth/login.js\n+++ b/src/auth/login.js\n@@ -1 +1 @@\n-old\n+new\n",
  });
  writeJson(patchPlanAbs, plan);

  writeJson(join(workDirAbs, "BUNDLE.json"), {
    version: 1,
    work_id: workId,
    bundle_hash: "deadbeef",
    repos: [{ repo_id: repoId, patch_plan_json_path: patchPlanRel }],
  });

  // Add invariants pack to knowledge repo.
  mkdirSync(join(knowledgeRootAbs, "qa"), { recursive: true });
  writeJson(join(knowledgeRootAbs, "qa", "invariants.json"), {
    version: 1,
    invariants: [
      {
        id: "INV_auth_001",
        text: "Authentication must reject invalid tokens.",
        severity: "high",
        keywords: ["auth", "login"],
        requires: { unit: true, integration: true, e2e: false },
        scopes: ["system"],
        sources: [],
      },
    ],
  });

  const res = await runQaObligations({ projectRoot: opsRootAbs, workId, dryRun: false });
  assert.equal(res.ok, true);
  assert.equal(res.obligations.must_add_unit, true);
  assert.equal(res.obligations.must_add_integration, true);
  assert.ok(Array.isArray(res.obligations.invariants_matched_by_repo));
  assert.ok(res.obligations.invariants_matched_by_repo.some((r) => r.repo_id === repoId && r.invariant_ids.includes("INV_auth_001")));
  assert.ok(res.obligations.suggested_test_directives.some((d) => String(d).includes("Invariant INV_auth_001")));
});
