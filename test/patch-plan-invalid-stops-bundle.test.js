import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, existsSync, readFileSync, mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
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

test("patch plan normalization removes absolute cwd/commands (portable JSON)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-patch-plan-invalid-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");
  process.env.AI_TEAM_LLM_STUB = "ok_proposal_invalid_patchplan_cwd";

  const reposBase = join(root, "repos");
  mkdirSync(reposBase, { recursive: true });

  const projectId = "proj-patch-plan-normalize";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["FrontendDP"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["FrontendDP"], sharedPacks: [] });

  // Minimal policies required by loader.
  mkdirSync(join(root, "ops", "ai", "lane_b"), { recursive: true });
  writeFileSync(join(root, "ops", "config", "POLICIES.json"), JSON.stringify({ version: 1, merge_strategy: "deep_merge", selectors: [], named: {} }, null, 2) + "\n", "utf8");
  writeFileSync(join(root, "ops", "ai", "lane_b", "DECISIONS_NEEDED.md"), "", "utf8");
  writeFileSync(
    join(root, "ops", "config", "LLM_PROFILES.json"),
    JSON.stringify(
      {
        version: 1,
        profiles: {
          "planner.code_generation": { provider: "openai", model: "stub-ok-invalid-patchplan" },
          qa_test_author: { provider: "openai", model: "stub-ok-invalid-patchplan" },
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
          {
            agent_id: "FrontendDP__planner__01",
            team_id: "FrontendDP",
            role: "planner",
            implementation: "llm",
            llm_profile: "planner.code_generation",
            capacity: 1,
            enabled: true,
          },
          {
            agent_id: "QA__strategist__01",
            team_id: "QA",
            role: "qa_strategist",
            implementation: "llm",
            llm_profile: "qa_test_author",
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

  // Project REPOS.json
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

  const workId = "W-2026-02-02T00:00:00.000Z-badc0d";
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
  assert.equal(result.ok, true, JSON.stringify(result, null, 2));

  const planPath = join(workDir, "patch-plans", "dp-frontend-portal.json");
  assert.ok(existsSync(planPath), "patch plan JSON must be written");

  const planText = readFileSync(planPath, "utf8");
  assert.ok(!planText.includes("/opt/"), "patch plan must not contain absolute /opt paths");
  assert.ok(!/[A-Za-z]:\\\\/.test(planText), "patch plan must not contain Windows absolute paths");
  assert.ok(!planText.includes("--prefix /"), "patch plan must not include npm --prefix /abs");
  assert.ok(!/\"diff\"\\s*:/.test(planText), "patch plan must not contain edits[].diff (forbidden)");

  const plan = JSON.parse(planText);
  assert.deepEqual(plan.target_branch, { name: "develop", source: "routing", confidence: 1 }, "patch plan must include target_branch from ROUTING.json");
  assert.equal(plan.commands.cwd, ".", "commands.cwd must be repo-relative '.'");
  assert.equal(plan.repo_path, "DP_Frontend-Portal");
  assert.ok(plan.edits?.[0]?.patch?.includes("diff --git a/README.md b/README.md"), "edits[].patch must be a unified diff derived by git");

  assert.ok(existsSync(join(workDir, "BUNDLE.json")), "BUNDLE.json must be created");
  const bundle = JSON.parse(readFileSync(join(workDir, "BUNDLE.json"), "utf8"));
  assert.ok(Array.isArray(bundle?.inputs?.qa_plan_jsons), "bundle.inputs.qa_plan_jsons must exist");
  assert.ok(existsSync(join(workDir, "SSOT_BUNDLE.json")), "work SSOT_BUNDLE.json must be created");
  assert.equal(bundle.ssot_bundle_path, `ai/lane_b/work/${workId}/SSOT_BUNDLE.json`, "bundle must include ssot_bundle_path");
  const ssotText = readFileSync(join(workDir, "SSOT_BUNDLE.json"), "utf8");
  const ssotSha = createHash("sha256").update(ssotText, "utf8").digest("hex");
  assert.equal(bundle.ssot_bundle_sha256, ssotSha, "bundle ssot_bundle_sha256 must match file content");

  const qaPlanPath = join(workDir, "qa", "qa-plan.dp-frontend-portal.json");
  assert.ok(existsSync(qaPlanPath), "qa plan json must be created");
  const qaPlanText = readFileSync(qaPlanPath, "utf8");
  const qaPlanSha = createHash("sha256").update(qaPlanText, "utf8").digest("hex");
  const qaPin = bundle.inputs.qa_plan_jsons.find((p) => p && p.path === `ai/lane_b/work/${workId}/qa/qa-plan.dp-frontend-portal.json`);
  assert.ok(qaPin, "bundle must pin qa plan json path");
  assert.equal(qaPin.sha256, qaPlanSha, "bundle qa pin sha must match file content");

  const status = await readWorkStatusSnapshot(workId);
  assert.ok(status.ok, "STATUS.md snapshot should parse");
  assert.ok(status.snapshot.current_stage !== "FAILED", "work should not be FAILED");
});
