import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { runKnowledgeSynthesize } from "../src/lane_a/knowledge/knowledge-synthesize.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("knowledge index+scan writes stable repo-scoped outputs (deterministic scan_version)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-scan-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  const projectId = "proj-scan";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const reposRoot = join(root, "repos");
  mkdirSync(reposRoot, { recursive: true });

  // Two minimal git repos.
  const repoAAbs = join(reposRoot, "repo-a");
  const repoBAbs = join(reposRoot, "repo-b");
  mkdirSync(join(repoAAbs, "src"), { recursive: true });
  mkdirSync(join(repoBAbs, "src"), { recursive: true });
  writeFileSync(join(repoAAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAAbs, "src", "index.js"), "console.log('a')\n", "utf8");
  writeFileSync(join(repoBAbs, "package.json"), JSON.stringify({ name: "repo-b", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoBAbs, "src", "index.js"), "console.log('b')\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAAbs).ok);
  assert.ok(run("git", ["init", "-q"], repoBAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoBAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoBAbs).ok);
  assert.ok(run("git", ["add", "."], repoAAbs).ok);
  assert.ok(run("git", ["add", "."], repoBAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoBAbs).ok);

  writeFileSync(
    join(opsRoot, "config", "REPOS.json"),
    JSON.stringify(
      {
        version: 1,
        repos: [
          { repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" },
          { repo_id: "repo-b", path: "repo-b", status: "active", team_id: "Tooling" },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  const idx1 = await runKnowledgeIndex({ projectRoot: opsRoot, dryRun: false });
  assert.equal(idx1.ok, true, JSON.stringify(idx1, null, 2));

  const scan1 = await runKnowledgeScan({ projectRoot: opsRoot, concurrency: 2, dryRun: false });
  assert.equal(scan1.ok, true, JSON.stringify(scan1, null, 2));
  assert.equal(scan1.scanned.length, 2);

  const scanAbs = join(knowledgeRepo.knowledgeRootAbs, "ssot", "repos", "repo-a", "scan.json");
  assert.equal(existsSync(scanAbs), true);
  const s1 = JSON.parse(readFileSync(scanAbs, "utf8"));
  const v1 = s1.scan_version;

  const scan2 = await runKnowledgeScan({ projectRoot: opsRoot, concurrency: 2, dryRun: false });
  assert.equal(scan2.ok, true);
  const s2 = JSON.parse(readFileSync(scanAbs, "utf8"));
  assert.equal(s2.scan_version, v1);
});

test("knowledge synthesize reads scan outputs and writes system integration + gaps under ssot/system", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-synth-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  const projectId = "proj-synth";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const reposRoot = join(root, "repos");
  mkdirSync(reposRoot, { recursive: true });
  const repoAAbs = join(reposRoot, "repo-a");
  mkdirSync(join(repoAAbs, "src"), { recursive: true });
  writeFileSync(join(repoAAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAAbs, "src", "index.js"), "console.log('a')\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAAbs).ok);
  assert.ok(run("git", ["add", "."], repoAAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAAbs).ok);

  writeFileSync(
    join(opsRoot, "config", "REPOS.json"),
    JSON.stringify({ version: 1, repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }] }, null, 2) + "\n",
    "utf8",
  );

  const idx = await runKnowledgeIndex({ projectRoot: opsRoot, dryRun: false });
  assert.equal(idx.ok, true);
  const scan = await runKnowledgeScan({ projectRoot: opsRoot, concurrency: 1, dryRun: false });
  assert.equal(scan.ok, true);

  const res = await runKnowledgeSynthesize({ projectRoot: opsRoot, dryRun: false });
  assert.equal(res.ok, true, JSON.stringify(res, null, 2));

  assert.equal(existsSync(join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "integration.json")), true);
  assert.equal(existsSync(join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "gaps.json")), true);
  const gaps = JSON.parse(readFileSync(join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "gaps.json"), "utf8"));
  assert.equal(gaps.scope, "system");
  assert.ok(Array.isArray(gaps.gaps));
});

test("knowledge scan fails cleanly when no repos configured", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-scan-norepos-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  const projectId = "proj-norepos";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  // No config/REPOS.json written.
  const res = await runKnowledgeScan({ projectRoot: opsRoot, dryRun: false });
  assert.equal(res.ok, false);
  assert.ok(typeof res.message === "string" && res.message.includes("config/REPOS.json"));
});
