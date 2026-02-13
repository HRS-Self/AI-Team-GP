import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { getOriginUrl, probeGitWorkTree } from "../src/lane_a/knowledge/git-checks.js";

function run(cmd, { cwd }) {
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function initRepo(cwd) {
  assert.ok(run("git init -q", { cwd }).ok);
  assert.ok(run('git config user.email "test@example.com"', { cwd }).ok);
  assert.ok(run('git config user.name "test"', { cwd }).ok);
  writeFileSync(join(cwd, "README.md"), "ok\n", "utf8");
  assert.ok(run("git add README.md", { cwd }).ok);
  assert.ok(run('git commit -m "init" -q', { cwd }).ok);
}

test("probeGitWorkTree accepts normal repo and worktree (.git file)", () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-git-checks-"));
  const repo = join(root, "repo");
  const wt = join(root, "worktree");
  mkdirSync(repo, { recursive: true });
  initRepo(repo);

  assert.ok(run(`git worktree add -q "${wt}" -b wtbranch`, { cwd: repo }).ok);

  const repoProbe = probeGitWorkTree({ cwd: repo });
  assert.equal(repoProbe.ok, true);
  assert.equal(repoProbe.is_inside_work_tree, true);
  assert.equal(repoProbe.dotgit.exists, true);
  assert.equal(repoProbe.dotgit.kind, "directory");

  const wtProbe = probeGitWorkTree({ cwd: wt });
  assert.equal(wtProbe.ok, true);
  assert.equal(wtProbe.is_inside_work_tree, true);
  assert.equal(wtProbe.dotgit.exists, true);
  assert.equal(wtProbe.dotgit.kind, "file");
});

test("getOriginUrl returns warning (non-fatal) when origin is missing", () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-git-origin-"));
  const repo = join(root, "repo");
  mkdirSync(repo, { recursive: true });
  initRepo(repo);

  const res = getOriginUrl({ cwd: repo });
  assert.equal(res.ok, false);
  assert.equal(res.url, null);
  assert.equal(res.warning?.code, "missing_origin");
  assert.ok(String(res.warning?.message || "").includes(`cwd: ${repo}`));
});

test("probeGitWorkTree failure message includes cwd and git stdout/stderr", () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-not-git-"));
  const dir = join(root, "not-a-repo");
  mkdirSync(dir, { recursive: true });

  const res = probeGitWorkTree({ cwd: dir });
  assert.equal(res.ok, false);
  assert.equal(res.is_inside_work_tree, false);
  assert.ok(String(res.message || "").includes(`cwd: ${dir}`));
  assert.ok(String(res.message || "").includes("stdout:"));
  assert.ok(String(res.message || "").includes("stderr:"));
});
