import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, readdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { runKnowledgeCommittee } from "../src/lane_a/knowledge/committee-runner.js";
import { runWriter } from "../src/writer/writer-runner.js";
import { loadProjectPaths } from "../src/paths/project-paths.js";
import { loadRepoRegistry } from "../src/utils/repo-registry.js";
import { evaluateRepoStaleness } from "../src/lane_a/lane-a-staleness-policy.js";

function run(cmd, args, cwd, env = null) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8", env: env ? { ...process.env, ...env } : process.env });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function writeJson(abs, obj) {
  writeFileSync(abs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function listDecisionPackets(knowledgeRootAbs, repoId) {
  const dir = join(knowledgeRootAbs, "decisions");
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((f) => f.startsWith(`DECISION-refresh-required-${repoId}-`) && f.endsWith(".json"))
    .map((f) => join(dir, f))
    .sort((a, b) => a.localeCompare(b));
}

function setupOneRepoProject() {
  const projectHomeAbs = mkdtempSync(join(tmpdir(), "ai-team-stale-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: projectHomeAbs, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: projectHomeAbs, projectId: "t", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const { opsRootAbs, reposRootAbs, knowledgeRootAbs } = knowledgeRepo;
  const repoId = "repo-a";
  const repoAbs = join(reposRootAbs, repoId);
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: repoId, main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('a')\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

  writeJson(join(opsRootAbs, "config", "REPOS.json"), { version: 1, repos: [{ repo_id: repoId, path: repoId, status: "active", team_id: "Tooling" }] });

  writeJson(join(opsRootAbs, "config", "LLM_PROFILES.json"), {
    version: 1,
    profiles: {
      "committee.repo_architect": { provider: "openai", model: "gpt-5.2-mini" },
      "committee.repo_skeptic": { provider: "openai", model: "gpt-5.2-mini" },
      "committee.integration_chair": { provider: "openai", model: "gpt-5.2-mini" },
      "writer.default": { provider: "openai", model: "gpt-5.2-mini" },
    },
  });

  writeJson(join(opsRootAbs, "config", "POLICIES.json"), { version: 1, policies: [] });
  writeJson(join(opsRootAbs, "config", "AGENTS.json"), {
    version: 3,
    agents: [
      { agent_id: "writer-1", enabled: true, role: "writer", implementation: "llm", llm_profile: "writer.default" },
    ],
  });
  writeJson(join(opsRootAbs, "config", "DOCS.json"), {
    version: 1,
    project_key: "t",
    docs_repo_path: knowledgeRootAbs,
    knowledge_repo_path: knowledgeRootAbs,
    max_docs_per_run: 1,
    output_format: "markdown",
    parts_word_target: 500,
    commit: { enabled: false },
  });

  return { projectHomeAbs, opsRootAbs, reposRootAbs, knowledgeRootAbs, repoId, repoAbs };
}

function appendMergeEvent({ opsRootAbs, repoId, timestampIso }) {
  const segmentsDir = join(opsRootAbs, "ai", "lane_a", "events", "segments");
  mkdirSync(segmentsDir, { recursive: true });
  const seg = join(segmentsDir, "events-20260210-00.jsonl");
  const obj = { version: 1, type: "merge", scope: `repo:${repoId}`, repo_id: repoId, timestamp: timestampIso, event_id: `KEVT_${repoId}` };
  writeFileSync(seg, JSON.stringify(obj) + "\n", { encoding: "utf8", flag: "a" });
}

test("committee refuses when hard stale and writes refresh-required decision packet (no LLM calls)", async (t) => {
  const { opsRootAbs, knowledgeRootAbs, repoId } = setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  delete process.env.AI_TEAM_LLM_STUB; // ensure no LLM stub; committee must not attempt LLM creation/invocation.
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
    if (typeof prevStub === "string") process.env.AI_TEAM_LLM_STUB = prevStub;
    else delete process.env.AI_TEAM_LLM_STUB;
  });

  const idx = await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(idx.ok, true);
  const scan = await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(scan.ok, true);

  appendMergeEvent({ opsRootAbs, repoId, timestampIso: new Date(Date.now() + 3600_000).toISOString() });
  {
    const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
    const reposRes = await loadRepoRegistry({ projectRoot: opsRootAbs });
    const s = await evaluateRepoStaleness({ paths, registry: reposRes.registry, repoId });
    assert.equal(s.hard_stale, true);
  }

  const res = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: `repo:${repoId}`, limit: 1, dryRun: false });
  assert.equal(res.ok, false);
  assert.equal(res.executed.length, 1);
  assert.equal(res.executed[0].type, "repo_committee");
  assert.equal(res.executed[0].results.length, 1);
  assert.equal(res.executed[0].results[0].reason_code, "STALE_BLOCKED");

  const packets = listDecisionPackets(knowledgeRootAbs, repoId);
  assert.ok(packets.length >= 1);
});

test("writer refuses when hard stale and writes refresh-required decision packet (no LLM calls)", async (t) => {
  const { opsRootAbs, knowledgeRootAbs, repoId } = setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  const prevCwd = process.cwd();
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  process.chdir(opsRootAbs);
  t.after(() => {
    process.chdir(prevCwd);
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  const idx = await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(idx.ok, true);
  const scan = await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(scan.ok, true);

  appendMergeEvent({ opsRootAbs, repoId, timestampIso: new Date(Date.now() + 3600_000).toISOString() });
  {
    const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
    const reposRes = await loadRepoRegistry({ projectRoot: opsRootAbs });
    const s = await evaluateRepoStaleness({ paths, registry: reposRes.registry, repoId });
    assert.equal(s.hard_stale, true);
  }

  const res = await runWriter({ projectRoot: opsRootAbs, scope: `repo:${repoId}`, docs: "00_Vision", dryRun: false });
  assert.equal(res.ok, false);
  assert.equal(res.reason_code, "STALE_BLOCKED");

  const packets = listDecisionPackets(knowledgeRootAbs, repoId);
  assert.ok(packets.length >= 1);
});

test("committee soft stale runs but emits stale=true in outputs", async (t) => {
  const { opsRootAbs, knowledgeRootAbs, repoId, repoAbs } = setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  process.env.AI_TEAM_LLM_STUB = "committee_all_pass";
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
    if (typeof prevStub === "string") process.env.AI_TEAM_LLM_STUB = prevStub;
    else delete process.env.AI_TEAM_LLM_STUB;
  });

  const idx = await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(idx.ok, true);
  const scan = await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(scan.ok, true);

  // Soft stale: advance HEAD, but do not emit merge events (merge_after_scan=false) and scan is recent.
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('a2')\n", "utf8");
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "c2"], repoAbs).ok);

  const res = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: `repo:${repoId}`, limit: 1, dryRun: false });
  assert.equal(res.ok, false);

  const archAbs = join(knowledgeRootAbs, "ssot", "repos", repoId, "committee", "architect_claims.json");
  assert.equal(existsSync(archAbs), true);
  const arch = JSON.parse(readFileSync(archAbs, "utf8"));
  assert.equal(arch.stale, true);
  const unknowns = Array.isArray(arch.unknowns) ? arch.unknowns : [];
  assert.ok(unknowns.some((u) => typeof u?.text === "string" && u.text.includes("stale")));
});
