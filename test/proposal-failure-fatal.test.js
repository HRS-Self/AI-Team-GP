import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, existsSync, readFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { runProposeBundle } from "../src/lane_b/agents/propose-bundle-runner.js";
import { readWorkStatusSnapshot } from "../src/utils/status-writer.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

test("LLM timeout during proposal writes PROPOSAL_FAILED.json and blocks bundle/patch-plans", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-proposal-fail-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");
  process.env.AI_TEAM_LLM_STUB = "timeout";

  const workId = "W-2026-02-02T00:00:00.000Z-deadbe";

  // Minimal project state
  const projectId = "proj-proposal-fail";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["FrontendDP"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["FrontendDP"], sharedPacks: [] });

  mkdirSync(join(root, "ops", "ai", "lane_b"), { recursive: true });
  writeFileSync(join(root, "ops", "config", "POLICIES.json"), JSON.stringify({ version: 1, merge_strategy: "deep_merge", selectors: [], named: {} }, null, 2) + "\n", "utf8");
  writeFileSync(join(root, "ops", "config", "REPOS.json"), JSON.stringify({ version: 1, repos: [] }, null, 2) + "\n", "utf8");
  writeFileSync(join(root, "ops", "config", "TEAMS.json"), JSON.stringify({ version: 1, teams: [{ team_id: "FrontendDP", name: "FrontendDP" }] }, null, 2) + "\n", "utf8");
  writeFileSync(join(root, "ops", "ai", "lane_b", "DECISIONS_NEEDED.md"), "", "utf8");
  writeFileSync(
    join(root, "ops", "config", "LLM_PROFILES.json"),
    JSON.stringify({ version: 1, profiles: { "planner.code_generation": { provider: "openai", model: "stub-timeout" } } }, null, 2) + "\n",
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

  const workDir = join(root, "ops", "ai", "lane_b", "work", workId);
  mkdirSync(join(workDir, "tasks"), { recursive: true });
  writeFileSync(join(workDir, "INTAKE.md"), "Update README in develop.\n", "utf8");
  writeFileSync(
    join(workDir, "ROUTING.json"),
    JSON.stringify({ workId, selected_teams: ["FrontendDP"], selected_repos: [], target_branch: { name: "develop", source: "explicit", confidence: 1.0, valid: true } }, null, 2) + "\n",
    "utf8",
  );
  writeFileSync(join(workDir, "tasks", "FrontendDP.md"), "Do the thing.\n", "utf8");

  const result = await runProposeBundle({ repoRoot: process.cwd(), workId, teamsCsv: "FrontendDP" });
  assert.equal(result.ok, false);

  const failedPath = join(workDir, "PROPOSAL_FAILED.json");
  assert.ok(existsSync(failedPath), "PROPOSAL_FAILED.json should exist");
  const failed = JSON.parse(readFileSync(failedPath, "utf8"));
  assert.equal(failed.stage, "PROPOSE");
  assert.equal(failed.status, "FAILED");
  assert.equal(failed.error_type, "timeout");

  assert.ok(!existsSync(join(workDir, "BUNDLE.json")), "BUNDLE.json must not exist");
  assert.ok(!existsSync(join(workDir, "patch-plans")), "patch-plans must not exist");

  const status = await readWorkStatusSnapshot(workId);
  assert.ok(status.ok, "STATUS.md snapshot should parse");
  assert.equal(status.snapshot.current_stage, "FAILED");
  assert.equal(status.snapshot.blocked, true);
  assert.equal(status.snapshot.blocking_reason, "PROPOSAL_FAILED");
});
