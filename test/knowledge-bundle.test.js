import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { runKnowledgeBundle } from "../src/lane_a/knowledge/knowledge-bundle.js";
import { runKnowledgeCommittee } from "../src/lane_a/knowledge/committee-runner.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function writeJson(abs, obj) {
  writeFileSync(abs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function setupTwoReposProject() {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-bundle-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "t", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const opsRootAbs = join(root, "ops");
  const reposRootAbs = join(root, "repos");

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

  return { root, opsRootAbs, reposRootAbs, knowledgeRootAbs: knowledgeRepo.knowledgeRootAbs };
}

test("--knowledge-bundle is deterministic and content-addressed for repo scope", async (t) => {
  const { opsRootAbs, knowledgeRootAbs } = setupTwoReposProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

  const b1 = await runKnowledgeBundle({ projectRoot: opsRootAbs, scope: "repo:repo-a", dryRun: false });
  assert.equal(b1.ok, true);
  assert.ok(b1.bundle_id.startsWith("sha256-"));

  const manifest1 = JSON.parse(readFileSync(join(b1.out_dir, "manifest.json"), "utf8"));
  const mpaths1 = (manifest1.files || []).map((f) => f.logical_path);
  assert.ok(mpaths1.some((p) => p === "ssot/repos/repo-a/scan.json"));
  assert.ok(!mpaths1.some((p) => p.includes("repo-b")));
  assert.ok(mpaths1.some((p) => p === "bundle/evidence_bundle.json"));

  const b2 = await runKnowledgeBundle({ projectRoot: opsRootAbs, scope: "repo:repo-a", dryRun: false });
  assert.equal(b2.ok, true);
  assert.equal(b2.bundle_id, b1.bundle_id);

  const manifest2 = readFileSync(join(b2.out_dir, "manifest.json"), "utf8");
  assert.equal(manifest2, readFileSync(join(b1.out_dir, "manifest.json"), "utf8"));

  // Bundle changes when an included SSOT section changes.
  const visionAbs = join(knowledgeRootAbs, "ssot", "system", "sections", "vision.json");
  const v = JSON.parse(readFileSync(visionAbs, "utf8"));
  v.content = `changed-${Date.now()}`;
  writeFileSync(visionAbs, JSON.stringify(v, null, 2) + "\n", "utf8");

  const b3 = await runKnowledgeBundle({ projectRoot: opsRootAbs, scope: "repo:repo-a", dryRun: false });
  assert.equal(b3.ok, true);
  assert.notEqual(b3.bundle_id, b1.bundle_id);
});

test("committee can consume a repo bundle without repo clone access", async (t) => {
  const { opsRootAbs, reposRootAbs } = setupTwoReposProject();
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

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

  const b = await runKnowledgeBundle({ projectRoot: opsRootAbs, scope: "repo:repo-a", dryRun: false });
  assert.equal(b.ok, true);

  // Remove repo clone directory to ensure committee doesn't rely on git show.
  rmSync(join(reposRootAbs, "repo-a"), { recursive: true, force: true });
  assert.equal(existsSync(join(reposRootAbs, "repo-a")), false);

  const res = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "repo:repo-a", bundleId: b.bundle_id, limit: 1, dryRun: false });
  assert.equal(res.ok, true);
});

test("--knowledge-bundle system includes views/integration_map.json and is deterministic", async (t) => {
  const { opsRootAbs } = setupTwoReposProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

  const b1 = await runKnowledgeBundle({ projectRoot: opsRootAbs, scope: "system", dryRun: false });
  assert.equal(b1.ok, true);
  const manifest1 = JSON.parse(readFileSync(join(b1.out_dir, "manifest.json"), "utf8"));
  const mpaths1 = (manifest1.files || []).map((f) => f.logical_path);
  assert.ok(mpaths1.includes("views/integration_map.json"));

  const b2 = await runKnowledgeBundle({ projectRoot: opsRootAbs, scope: "system", dryRun: false });
  assert.equal(b2.ok, true);
  assert.equal(b2.bundle_id, b1.bundle_id);
  assert.equal(readFileSync(join(b2.out_dir, "manifest.json"), "utf8"), readFileSync(join(b1.out_dir, "manifest.json"), "utf8"));
});

test("integration chair refuses (evidence_invalid) when system bundle lacks views/integration_map.json", async (t) => {
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

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

  // Remove integration_map before bundling system to simulate missing bundle input.
  rmSync(join(knowledgeRootAbs, "views", "integration_map.json"), { force: true });
  const b = await runKnowledgeBundle({ projectRoot: opsRootAbs, scope: "system", dryRun: false });
  assert.equal(b.ok, true);

  // First run: generate repo committees.
  const r1 = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "system", limit: 2, dryRun: false });
  assert.equal(r1.ok, true);

  // Second run: integration chair should refuse due to missing integration_map in the selected system bundle.
  const r2 = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "system", bundleId: b.bundle_id, limit: 2, dryRun: false });
  assert.equal(r2.ok, false);

  const findingsAbs = join(knowledgeRootAbs, "ssot", "system", "committee", "integration", "integration_findings.json");
  const statusAbs = join(knowledgeRootAbs, "ssot", "system", "committee", "integration", "integration_status.json");
  assert.equal(existsSync(findingsAbs), true);
  assert.equal(existsSync(statusAbs), true);
  const findings = JSON.parse(readFileSync(findingsAbs, "utf8"));
  assert.equal(findings.scope, "system");
  assert.equal(findings.verdict, "evidence_invalid");
  const unknowns = Array.isArray(findings.unknowns) ? findings.unknowns : [];
  const missing = unknowns.flatMap((u) => (Array.isArray(u.evidence_missing) ? u.evidence_missing : []));
  assert.ok(missing.some((s) => String(s).includes("integration_map.json")));
});
