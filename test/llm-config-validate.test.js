import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Orchestrator } from "../src/lane_b/orchestrator-lane-b.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

test("validate enforces LLM_PROFILES.json + llm_profile-only agents (no model fallback)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-llm-validate-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");
  mkdirSync(join(root, "ops", "config"), { recursive: true });
  mkdirSync(join(root, "ops", "ai"), { recursive: true });

  const projectId = "proj-llm-validate";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["FrontendDP"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["FrontendDP"], sharedPacks: [] });

  // Required base files
  writeJson(join(root, "ops", "config", "POLICIES.json"), { version: 1, merge_strategy: "deep_merge", selectors: [], named: {} });
  writeJson(join(root, "ops", "config", "REPOS.json"), { version: 1, repos: [] });
  writeJson(join(root, "ops", "config", "TEAMS.json"), { version: 1, teams: [{ team_id: "FrontendDP", description: "", scope_hints: ["dp"], risk_level: "normal" }] });
  writeJson(join(root, "ops", "config", "AGENTS.json"), {
    version: 3,
    agents: [{ agent_id: "FrontendDP__planner__01", team_id: "FrontendDP", role: "planner", implementation: "llm", llm_profile: "planner.code_generation", capacity: 1, enabled: true }],
  });

  const orch = new Orchestrator({ repoRoot: process.cwd(), projectRoot: join(root, "ops") });

  // Missing LLM_PROFILES.json must fail.
  {
    const res = await orch.validate();
    assert.equal(res.ok, false);
    assert.ok(res.errors.some((e) => String(e).toLowerCase().includes("llm_profiles.json missing")));
  }

  // Add profiles; legacy agents with 'model' must fail.
  writeJson(join(root, "ops", "config", "LLM_PROFILES.json"), { version: 1, profiles: { "planner.code_generation": { provider: "openai", model: "stub" } } });
  writeJson(join(root, "ops", "config", "AGENTS.json"), {
    version: 3,
    agents: [{ agent_id: "FrontendDP__planner__01", team_id: "FrontendDP", role: "planner", implementation: "llm", llm_profile: "planner.code_generation", model: "gpt-x", capacity: 1, enabled: true }],
  });
  {
    const res = await orch.validate();
    assert.equal(res.ok, false);
    assert.ok(res.errors.some((e) => String(e).toLowerCase().includes("legacy key 'model'")));
  }

  // Unknown llm_profile must fail.
  writeJson(join(root, "ops", "config", "AGENTS.json"), {
    version: 3,
    agents: [{ agent_id: "FrontendDP__planner__01", team_id: "FrontendDP", role: "planner", implementation: "llm", llm_profile: "unknown.profile", capacity: 1, enabled: true }],
  });
  {
    const res = await orch.validate();
    assert.equal(res.ok, false);
    assert.ok(res.errors.some((e) => String(e).includes("unknown llm_profile")));
  }

  // Valid config passes.
  writeJson(join(root, "ops", "config", "AGENTS.json"), {
    version: 3,
    agents: [{ agent_id: "FrontendDP__planner__01", team_id: "FrontendDP", role: "planner", implementation: "llm", llm_profile: "planner.code_generation", capacity: 1, enabled: true }],
  });
  {
    const res = await orch.validate();
    assert.equal(res.ok, true, res.errors.join("\n"));
  }
});
