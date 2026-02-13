import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, existsSync, readFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { runProposeBundle } from "../src/lane_b/agents/propose-bundle-runner.js";
import { readWorkStatusSnapshot } from "../src/utils/status-writer.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function run(cmd, { cwd }) {
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("invalid patch plan (path traversal in edits[].path) is fatal: no plan file, no bundle, failure report written", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-patch-plan-invalid-path-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");
  process.env.AI_TEAM_LLM_STUB = "ok_proposal_invalid_patchplan_path";

  const reposBase = join(root, "repos");
  mkdirSync(reposBase, { recursive: true });

  const projectId = "proj-patch-plan-invalid-path";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["FrontendDP"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["FrontendDP"], sharedPacks: [] });

  mkdirSync(join(root, "ops", "ai", "lane_b"), { recursive: true });
  writeFileSync(join(root, "ops", "config", "POLICIES.json"), JSON.stringify({ version: 1, merge_strategy: "deep_merge", selectors: [], named: {} }, null, 2) + "\n", "utf8");
  writeFileSync(join(root, "ops", "ai", "lane_b", "DECISIONS_NEEDED.md"), "", "utf8");
  writeFileSync(
    join(root, "ops", "config", "LLM_PROFILES.json"),
    JSON.stringify({ version: 1, profiles: { "planner.code_generation": { provider: "openai", model: "stub-ok-invalid-patchplan-path" } } }, null, 2) + "\n",
    "utf8",
  );
  writeFileSync(
    join(root, "ops", "config", "AGENTS.json"),
    JSON.stringify(
      {
        version: 3,
        agents: [
          {
            agent_id: "FrontendDP__planner__01",
            team_id: "FrontendDP",
            role: "planner",
            implementation: "llm",
            llm_profile: "planner.code_generation",
            capacity: 1,
            enabled: true,
          },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  const repoAbs = join(reposBase, "DP_Frontend-Portal");
  mkdirSync(repoAbs, { recursive: true });
  assert.ok(run("git init -q", { cwd: repoAbs }).ok);
  assert.ok(run('git config user.email "test@example.com"', { cwd: repoAbs }).ok);
  assert.ok(run('git config user.name "test"', { cwd: repoAbs }).ok);
  writeFileSync(join(repoAbs, "README.md"), "test\n", "utf8");
  assert.ok(run("git add README.md", { cwd: repoAbs }).ok);
  assert.ok(run('git commit -m "init" -q', { cwd: repoAbs }).ok);
  assert.ok(run("git branch -m develop -q", { cwd: repoAbs }).ok);

  const reposJson = {
    version: 1,
    repos: [
      {
        repo_id: "dp-frontend-portal",
        name: "DP_Frontend-Portal",
        path: "DP_Frontend-Portal",
        status: "active",
        team_id: "FrontendDP",
        Kind: "App",
        IsHexa: false,
        commands: { cwd: ".", package_manager: "npm", install: null, lint: null, test: null, build: null },
      },
    ],
  };
  writeFileSync(join(root, "ops", "config", "REPOS.json"), JSON.stringify(reposJson, null, 2) + "\n", "utf8");

  const workId = "W-2026-02-02T00:00:00.000Z-badbad";
  const workDir = join(root, "ops", "ai", "lane_b", "work", workId);
  mkdirSync(join(workDir, "tasks"), { recursive: true });
  writeFileSync(join(workDir, "INTAKE.md"), "Update README in develop.\n", "utf8");
  writeFileSync(
    join(workDir, "ROUTING.json"),
    JSON.stringify(
      {
        workId,
        routing_mode: "repo_explicit",
        selected_teams: ["FrontendDP"],
        selected_repos: ["dp-frontend-portal"],
        target_branch: { name: "develop", source: "explicit", confidence: 1.0, valid: true },
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );
  writeFileSync(join(workDir, "tasks", "FrontendDP.md"), "Do the thing.\n", "utf8");

  const result = await runProposeBundle({ repoRoot: process.cwd(), workId, teamsCsv: "FrontendDP" });
  assert.equal(result.ok, false);

  const perRepoReportPath = join(workDir, "failure-reports", "patch-plan_dp-frontend-portal.md");
  assert.ok(existsSync(perRepoReportPath), "per-repo patch plan failure report must exist");

  const reportPath = join(workDir, "failure-reports", "patch-plan-validation.md");
  assert.ok(existsSync(reportPath), "patch plan validation report must exist");
  assert.ok(readFileSync(reportPath, "utf8").includes("path traversal"), "report should include path traversal error");

  assert.ok(!existsSync(join(workDir, "BUNDLE.json")), "BUNDLE.json must not exist");
  assert.ok(!existsSync(join(workDir, "patch-plans", "dp-frontend-portal.json")), "invalid patch plan JSON must not be written");

  const status = await readWorkStatusSnapshot(workId);
  assert.ok(status.ok, "STATUS.md snapshot should parse");
  assert.equal(status.snapshot.current_stage, "FAILED");
  assert.equal(status.snapshot.blocked, true);
  assert.equal(status.snapshot.blocking_reason, "PATCH_PLAN_INVALID");
});
