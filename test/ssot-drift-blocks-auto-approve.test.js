import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, existsSync, readFileSync, mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { runProposeBundle } from "../src/lane_b/agents/propose-bundle-runner.js";
import { Orchestrator } from "../src/lane_b/orchestrator-lane-b.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function run(cmd, { cwd }) {
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("SSOT drift hard violations prevent auto-approval (approvalGate forces manual)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-ssot-drift-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");
  process.env.AI_TEAM_LLM_STUB = "ok_proposal_invalid_patchplan_cwd";

  const reposBase = join(root, "repos");
  mkdirSync(reposBase, { recursive: true });

  const projectId = "proj-ssot-drift";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["FrontendDP"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["FrontendDP"], sharedPacks: [] });

  // Modify SSOT constraints section to forbid README.md edits (and update snapshot sha).
  const constraintsAbs = join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "sections", "constraints.json");
  const constraintsObj = { version: 1, id: "constraints", forbidden_paths: ["README.md"] };
  const constraintsText = JSON.stringify(constraintsObj, null, 2) + "\n";
  writeFileSync(constraintsAbs, constraintsText, "utf8");
  const newConstraintsSha = sha256Hex(constraintsText);

  const snapshotAbs = join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "PROJECT_SNAPSHOT.json");
  const snapshot = JSON.parse(readFileSync(snapshotAbs, "utf8"));
  snapshot.sections = snapshot.sections.map((s) => (s && s.id === "constraints" ? { ...s, sha256: newConstraintsSha } : s));
  writeFileSync(snapshotAbs, JSON.stringify(snapshot, null, 2) + "\n", "utf8");

  // Minimal policies enabling auto-approve, so drift is the only disqualifier.
  mkdirSync(join(root, "ops", "ai", "lane_b"), { recursive: true });
  writeFileSync(
    join(root, "ops", "config", "POLICIES.json"),
    JSON.stringify(
      {
        version: 1,
        merge_strategy: "deep_merge",
        approval: {
          auto_approve: {
            enabled: true,
            allowed_teams: ["FrontendDP"],
            allowed_kinds: ["App", "Package"],
            disallowed_risk_levels: ["high"],
            require_clean_patch_plan: true,
          },
          require_clean_patch_plan: true,
        },
        selectors: [],
        named: {},
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );
  writeFileSync(
    join(root, "ops", "config", "TEAMS.json"),
    JSON.stringify({ version: 1, teams: [{ team_id: "FrontendDP", name: "FrontendDP" }] }, null, 2) + "\n",
    "utf8",
  );
  writeFileSync(join(root, "ops", "ai", "lane_b", "DECISIONS_NEEDED.md"), "", "utf8");

  writeFileSync(
    join(root, "ops", "config", "LLM_PROFILES.json"),
    JSON.stringify(
      {
        version: 1,
        profiles: {
          "planner.code_generation": { provider: "openai", model: "stub-ok-drift" },
          qa_test_author: { provider: "openai", model: "stub-ok-drift" },
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

  const workId = "W-2026-02-02T00:00:00.000Z-drift00";
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

  const proposeRes = await runProposeBundle({ repoRoot: process.cwd(), workId, teamsCsv: "FrontendDP" });
  assert.equal(proposeRes.ok, true, JSON.stringify(proposeRes, null, 2));
  assert.ok(existsSync(join(workDir, "SSOT_BUNDLE.json")), "work SSOT_BUNDLE.json must exist after propose");

  const orchestrator = new Orchestrator({ repoRoot: process.cwd(), projectRoot: join(root, "ops") });
  const approvalRes = await orchestrator.approvalGate({ workId });
  assert.equal(approvalRes.ok, true, JSON.stringify(approvalRes, null, 2));
  assert.equal(approvalRes.mode, "manual", "auto-approval must be refused due to SSOT drift hard violations");
  assert.equal(approvalRes.status, "pending", "approval status must remain pending");

  const driftPath = join(workDir, "SSOT_DRIFT.json");
  assert.ok(existsSync(driftPath), "SSOT_DRIFT.json must be written");
  const drift = JSON.parse(readFileSync(driftPath, "utf8"));
  assert.ok(Array.isArray(drift.hard_violations) && drift.hard_violations.length >= 1, "hard_violations must be non-empty");
});
