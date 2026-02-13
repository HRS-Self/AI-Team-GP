import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { runKnowledgeCommittee } from "../src/lane_a/knowledge/committee-runner.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function setupTwoReposProject() {
  const projectHomeAbs = mkdtempSync(join(tmpdir(), "ai-team-committee-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: projectHomeAbs, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: projectHomeAbs, projectId: "t", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const { opsRootAbs, reposRootAbs, knowledgeRootAbs } = knowledgeRepo;

  const repoA = join(reposRootAbs, "repo-a");
  const repoB = join(reposRootAbs, "repo-b");
  mkdirSync(join(repoA, "src"), { recursive: true });
  mkdirSync(join(repoB, "src"), { recursive: true });
  writeFileSync(join(repoA, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoB, "package.json"), JSON.stringify({ name: "repo-b", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoA, "src", "index.js"), "console.log('a')\n", "utf8");
  writeFileSync(join(repoB, "src", "index.js"), "console.log('b')\n", "utf8");
  writeFileSync(join(repoA, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: A\n  version: 1\n", "utf8");
  writeFileSync(join(repoB, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: B\n  version: 1\n", "utf8");

  for (const repoAbs of [repoA, repoB]) {
    assert.ok(run("git", ["init", "-q"], repoAbs).ok);
    assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
    assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
    assert.ok(run("git", ["add", "."], repoAbs).ok);
    assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);
  }

  writeJson(join(opsRootAbs, "config", "REPOS.json"), {
    version: 1,
    repos: [
      { repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" },
      { repo_id: "repo-b", path: "repo-b", status: "active", team_id: "Tooling" },
    ],
  });

  writeJson(join(opsRootAbs, "config", "LLM_PROFILES.json"), {
    version: 1,
    profiles: {
      "committee.repo_architect": { provider: "openai", model: "gpt-5.2-mini" },
      "committee.repo_skeptic": { provider: "openai", model: "gpt-5.2-mini" },
      "committee.integration_chair": { provider: "openai", model: "gpt-5.2-mini" },
    },
  });

  return { opsRootAbs, knowledgeRootAbs };
}

test("committee does not run integration if one repo fails evidence_valid", async (t) => {
  const { opsRootAbs, knowledgeRootAbs } = setupTwoReposProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  process.env.AI_TEAM_LLM_STUB = "committee_repo_fail:repo-b";
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

  const res = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "system", limit: 2, dryRun: false });
  assert.equal(res.ok, false);

  const integAbs = join(knowledgeRootAbs, "ssot", "system", "committee", "integration", "integration_status.json");
  assert.equal(existsSync(integAbs), false);
});

test("committee rejects evidence-less claims and writes repo committee_status as evidence_valid=false", async (t) => {
  const { opsRootAbs, knowledgeRootAbs } = setupTwoReposProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  process.env.AI_TEAM_LLM_STUB = "committee_architect_no_evidence";
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

  const res = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "repo:repo-a", limit: 1, dryRun: false });
  assert.equal(res.ok, false);

  const stAbs = join(knowledgeRootAbs, "ssot", "repos", "repo-a", "committee", "committee_status.json");
  assert.equal(existsSync(stAbs), true);
  const st = JSON.parse(readFileSync(stAbs, "utf8"));
  assert.equal(st.repo_id, "repo-a");
  assert.equal(st.evidence_valid, false);
  assert.ok(Array.isArray(st.blocking_issues) && st.blocking_issues.length >= 1);
});

test("committee stable ids remain stable across reruns (claim ids)", async (t) => {
  const { opsRootAbs, knowledgeRootAbs } = setupTwoReposProject();
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

  const r1 = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "repo:repo-a", limit: 1, dryRun: false });
  assert.equal(r1.ok, true);
  const archAbs = join(knowledgeRootAbs, "ssot", "repos", "repo-a", "committee", "architect_claims.json");
  const j1 = JSON.parse(readFileSync(archAbs, "utf8"));
  const facts1 = (Array.isArray(j1.facts) ? j1.facts : []).map((f) => String(f.text || "").trim()).filter(Boolean).sort();
  assert.ok(facts1.length >= 1);

  // Remove committee_status to force rerun (deterministic).
  writeFileSync(join(knowledgeRootAbs, "ssot", "repos", "repo-a", "committee", "committee_status.json"), "", "utf8");

  const r2 = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "repo:repo-a", limit: 1, dryRun: false });
  assert.equal(r2.ok, true);
  const j2 = JSON.parse(readFileSync(archAbs, "utf8"));
  const facts2 = (Array.isArray(j2.facts) ? j2.facts : []).map((f) => String(f.text || "").trim()).filter(Boolean).sort();
  assert.deepEqual(facts2, facts1);
});
