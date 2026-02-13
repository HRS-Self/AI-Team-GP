import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { runProposeBundle } from "../src/lane_b/agents/propose-bundle-runner.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function run(cmd, { cwd }) {
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("patch plan generation refuses when ROUTING.json lacks target_branch", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-patch-plan-missing-branch-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");
  process.env.AI_TEAM_LLM_STUB = "ok_proposal_invalid_patchplan_cwd";

  const reposBase = join(root, "repos");
  mkdirSync(reposBase, { recursive: true });

  const projectId = "proj-patch-plan-missing-branch";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["FrontendDP"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["FrontendDP"], sharedPacks: [] });

  mkdirSync(join(root, "ops", "ai", "lane_b"), { recursive: true });

  writeFileSync(join(root, "ops", "config", "POLICIES.json"), JSON.stringify({ version: 1, merge_strategy: "deep_merge", selectors: [], named: {} }, null, 2) + "\n", "utf8");
  writeFileSync(join(root, "ops", "ai", "lane_b", "DECISIONS_NEEDED.md"), "", "utf8");
  writeFileSync(
    join(root, "ops", "config", "LLM_PROFILES.json"),
    JSON.stringify(
      {
        version: 1,
        profiles: {
          "planner.code_generation": { provider: "openai", model: "stub-missing-branch" },
          qa_test_author: { provider: "openai", model: "stub-missing-branch" },
        },
      },
      null,
      2,
    ) + "\n",
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
      { repo_id: "dp-frontend-portal", name: "DP_Frontend-Portal", path: "DP_Frontend-Portal", status: "active", team_id: "FrontendDP", Kind: "App", IsHexa: false, commands: { cwd: ".", package_manager: "npm", install: null, lint: null, test: null, build: null } },
    ],
  };
  writeFileSync(join(root, "ops", "config", "REPOS.json"), JSON.stringify(reposJson, null, 2) + "\n", "utf8");

  const workId = "W-2026-02-02T00:00:00.000Z-no-branch";
  const workDir = join(root, "ops", "ai", "lane_b", "work", workId);
  mkdirSync(join(workDir, "tasks"), { recursive: true });
  writeFileSync(join(workDir, "INTAKE.md"), "Update README.\n", "utf8");
  writeFileSync(
    join(workDir, "ROUTING.json"),
    JSON.stringify(
      {
        workId,
        routing_mode: "repo_explicit",
        selected_teams: ["FrontendDP"],
        selected_repos: ["dp-frontend-portal"],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );
  writeFileSync(join(workDir, "tasks", "FrontendDP.md"), "Do the thing.\n", "utf8");

  const result = await runProposeBundle({ repoRoot: process.cwd(), workId, teamsCsv: "FrontendDP" });
  assert.equal(result.ok, false);
  assert.ok(String(result.message || "").includes("routing.target_branch"), "error must mention missing routing.target_branch");

  assert.ok(!existsSync(join(workDir, "patch-plans", "dp-frontend-portal.json")), "patch plan JSON must not be written");
  assert.ok(!existsSync(join(workDir, "BUNDLE.json")), "bundle must not be created");
});
