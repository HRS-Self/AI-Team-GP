import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, existsSync } from "node:fs";
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

function initMinimalGitRepo(repoAbs) {
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "x", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('x')\n", "utf8");
  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);
}

test("knowledge-synthesize refuses when any active repo is missing scan.json", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-synth-missing-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  const projectId = "proj-synth-missing";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const reposRoot = join(root, "repos");
  mkdirSync(reposRoot, { recursive: true });
  initMinimalGitRepo(join(reposRoot, "repo-a"));
  initMinimalGitRepo(join(reposRoot, "repo-b"));

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

  // Index both, scan only one.
  const idx = await runKnowledgeIndex({ projectRoot: opsRoot, dryRun: false });
  assert.equal(idx.ok, true);
  const scan = await runKnowledgeScan({ projectRoot: opsRoot, repoId: "repo-a", concurrency: 1, dryRun: false });
  assert.equal(scan.ok, true);
  assert.equal(existsSync(join(knowledgeRepo.knowledgeRootAbs, "ssot", "repos", "repo-a", "scan.json")), true);
  assert.equal(existsSync(join(knowledgeRepo.knowledgeRootAbs, "ssot", "repos", "repo-b", "scan.json")), false);

  const synth = await runKnowledgeSynthesize({ projectRoot: opsRoot, dryRun: false });
  assert.equal(synth.ok, false);
  assert.ok(String(synth.message || "").includes("missing scan outputs"));
});
