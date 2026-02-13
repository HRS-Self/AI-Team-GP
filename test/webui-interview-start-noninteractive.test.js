import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, existsSync, readFileSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function writeJson(pathAbs, obj) {
  const text = typeof obj === "string" ? obj : JSON.stringify(obj, null, 2) + "\n";
  writeFileSync(pathAbs, text, "utf8");
}

test("Web-style start interview (stdin ignored) does not prompt and writes a session transcript", () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-webui-interview-start-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  mkdirSync(join(opsRoot, "ai", "lane_b"), { recursive: true });
  mkdirSync(join(opsRoot, "config"), { recursive: true });

  const projectId = "proj-webui";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  // Minimal mandatory project files for LLM profile resolution.
  writeJson(join(opsRoot, "ai", "lane_b", "DECISIONS_NEEDED.md"), "");
  writeJson(join(opsRoot, "config", "POLICIES.json"), { version: 1, merge_strategy: "deep_merge", selectors: [], named: {} });
  writeJson(join(opsRoot, "config", "TEAMS.json"), { version: 1, teams: [{ team_id: "Tooling", description: "t", scope_hints: [], risk_level: "normal" }] });
  writeJson(join(opsRoot, "config", "REPOS.json"), { version: 1, repos: [] });
  writeJson(join(opsRoot, "config", "LLM_PROFILES.json"), { version: 1, profiles: { "architect.interviewer": { provider: "openai", model: "stub" } } });
  writeJson(join(opsRoot, "config", "AGENTS.json"), {
    version: 3,
    agents: [{ agent_id: "Tooling__interviewer__01", team_id: "Tooling", role: "interviewer", implementation: "llm", llm_profile: "architect.interviewer", capacity: 1, enabled: true }],
  });

  const res = spawnSync(process.execPath, ["src/cli.js", "--knowledge-interview", "--projectRoot", opsRoot, "--scope", "system", "--start"], {
    cwd: resolve("/opt/GitRepos/AI-Team"),
    env: { ...process.env, KNOWLEDGE_TEST_STUB: "1" },
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });

  assert.equal(res.status, 0, `expected exit=0, got ${res.status}\nstdout:\n${res.stdout}\nstderr:\n${res.stderr}`);
  assert.ok(!String(res.stdout || "").includes("Scope (in/out):"), "must not prompt in non-interactive mode");

  const sessionsDir = join(knowledgeRepo.knowledgeRootAbs, "sessions");
  assert.equal(existsSync(sessionsDir), true, "expected knowledge/sessions dir to exist");
  const sessionFiles = readdirSync(sessionsDir).filter((n) => n.startsWith("SESSION-") && n.endsWith(".md"));
  assert.ok(sessionFiles.length >= 1, "expected at least one SESSION-*.md");
  const transcript = readFileSync(join(sessionsDir, sessionFiles.sort()[0]), "utf8");
  assert.ok(transcript.includes("# Architect Interview"));

  const assumptionsAbs = join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "assumptions.json");
  assert.equal(existsSync(assumptionsAbs), true, "expected assumptions.json");
  assert.equal(existsSync(join(opsRoot, "ai", "knowledge")), false, "expected no runtime pointer folder ai/knowledge");
});
