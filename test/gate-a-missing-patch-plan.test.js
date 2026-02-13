import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, existsSync, readFileSync, mkdirSync, unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { runProposeBundle } from "../src/lane_b/agents/propose-bundle-runner.js";
import { requestApplyApproval } from "../src/lane_b/gates/apply-approval.js";
import { readWorkStatusSnapshot } from "../src/utils/status-writer.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function run(cmd, { cwd }) {
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("Apply approval cannot approve when patch plans are missing; stage remains APPLY_APPROVAL_PENDING", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-gate-a-missing-plan-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");
  process.env.AI_TEAM_LLM_STUB = "ok_proposal_invalid_patchplan_cwd";

  const reposBase = join(root, "repos");
  mkdirSync(reposBase, { recursive: true });

  const projectId = "proj-gate-a";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["FrontendDP"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["FrontendDP"], sharedPacks: [] });

  writeFileSync(join(root, "ops", "config", "POLICIES.json"), JSON.stringify({ version: 1, merge_strategy: "deep_merge", selectors: [], named: {} }, null, 2) + "\n", "utf8");
  mkdirSync(join(root, "ops", "ai", "lane_b"), { recursive: true });
  writeFileSync(join(root, "ops", "ai", "lane_b", "DECISIONS_NEEDED.md"), "", "utf8");
  writeFileSync(
    join(root, "ops", "config", "LLM_PROFILES.json"),
    JSON.stringify({ version: 1, profiles: { "planner.code_generation": { provider: "openai", model: "stub" }, qa_test_author: { provider: "openai", model: "stub" } } }, null, 2) + "\n",
    "utf8",
  );
  writeFileSync(
    join(root, "ops", "config", "AGENTS.json"),
    JSON.stringify(
      {
        version: 3,
        agents: [
          { agent_id: "FrontendDP__planner__01", team_id: "FrontendDP", role: "planner", implementation: "llm", llm_profile: "planner.code_generation", capacity: 1, enabled: true },
          { agent_id: "QA__strategist__01", team_id: "QA", role: "qa_strategist", implementation: "llm", llm_profile: "qa_test_author", capacity: 1, enabled: true },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  // Create a local git repo with a develop branch.
  const repoAbs = join(reposBase, "DP_Frontend-Portal");
  mkdirSync(repoAbs, { recursive: true });
  assert.ok(run("git init -q", { cwd: repoAbs }).ok);
  assert.ok(run('git config user.email "test@example.com"', { cwd: repoAbs }).ok);
  assert.ok(run('git config user.name "test"', { cwd: repoAbs }).ok);
  writeFileSync(join(repoAbs, "README.md"), "test\n", "utf8");
  assert.ok(run("git add README.md", { cwd: repoAbs }).ok);
  assert.ok(run('git commit -m "init" -q', { cwd: repoAbs }).ok);
  assert.ok(run("git branch -m develop -q", { cwd: repoAbs }).ok);

  writeFileSync(
    join(root, "ops", "config", "REPOS.json"),
    JSON.stringify(
      {
        version: 1,
        repos: [
          { repo_id: "dp-frontend-portal", name: "DP_Frontend-Portal", path: "DP_Frontend-Portal", status: "active", team_id: "FrontendDP", Kind: "App", IsHexa: false, commands: { cwd: ".", package_manager: "npm", install: null, lint: null, test: null, build: null } },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  const workId = "W-2026-02-05T00:00:00.000Z-gatea";
  const workDir = join(root, "ops", "ai", "lane_b", "work", workId);
  mkdirSync(join(workDir, "tasks"), { recursive: true });
  writeFileSync(join(workDir, "META.json"), JSON.stringify({ version: 1, work_id: workId }, null, 2) + "\n", "utf8");
  writeFileSync(join(workDir, "INTAKE.md"), "Update README in develop.\n", "utf8");
  writeFileSync(
    join(workDir, "ROUTING.json"),
    JSON.stringify({ workId, routing_mode: "repo_explicit", selected_teams: ["FrontendDP"], selected_repos: ["dp-frontend-portal"], target_branch: { name: "develop", source: "explicit", confidence: 1.0, valid: true } }, null, 2) + "\n",
    "utf8",
  );
  writeFileSync(join(workDir, "tasks", "FrontendDP.md"), "Do the thing.\n", "utf8");

  const proposeRes = await runProposeBundle({ repoRoot: process.cwd(), workId, teamsCsv: "FrontendDP" });
  assert.equal(proposeRes.ok, true, JSON.stringify(proposeRes, null, 2));

  const planPath = join(workDir, "patch-plans", "dp-frontend-portal.json");
  assert.ok(existsSync(planPath), "patch plan must exist before removing");
  unlinkSync(planPath);

  const gateRes = await requestApplyApproval({ workId });
  assert.equal(gateRes.ok, true);
  assert.notEqual(gateRes.status, "approved");
  assert.ok(existsSync(join(workDir, "APPLY_APPROVAL.json")), "APPLY_APPROVAL.json must be written");
  const gate = JSON.parse(readFileSync(join(workDir, "APPLY_APPROVAL.json"), "utf8"));
  assert.equal(gate.version, 1);
  assert.ok(Array.isArray(gate.reason_codes));

  const status = await readWorkStatusSnapshot(workId);
  assert.ok(status.ok);
  assert.equal(status.snapshot.current_stage, "APPLY_APPROVAL_PENDING");
});
