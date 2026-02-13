import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

test("knowledge-index fails fast when repo is not a git worktree and writes an ops lane_a error artifact", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-index-nogit-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  mkdirSync(join(opsRoot, "config"), { recursive: true });

  const projectId = "proj-index-nogit";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  // Create a non-git repo directory under REPOS_ROOT.
  const reposRoot = join(root, "repos");
  const repoAbs = join(reposRoot, "repo-nogit");
  mkdirSync(repoAbs, { recursive: true });
  writeFileSync(join(repoAbs, "README.md"), "# hello\n", "utf8");

  writeJson(join(opsRoot, "config", "REPOS.json"), {
    version: 1,
    repos: [{ repo_id: "repo-nogit", path: "repo-nogit", status: "active", team_id: "Tooling" }],
  });

  const res = await runKnowledgeIndex({ projectRoot: opsRoot, dryRun: false });
  assert.equal(res.ok, false);
  assert.ok(Array.isArray(res.failed) && res.failed.length === 1);
  assert.equal(res.failed[0].repo_id, "repo-nogit");

  const errAbs = join(opsRoot, "ai", "lane_a", "logs", "knowledge-index__repo-nogit.error.json");
  assert.equal(existsSync(errAbs), true);
  const errJson = JSON.parse(readFileSync(errAbs, "utf8"));
  assert.equal(errJson.repo_id, "repo-nogit");
  assert.ok(String(errJson.message || "").includes("Not a git worktree"));
});
