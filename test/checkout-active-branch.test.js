import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, readFileSync } from "node:fs";
import { mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { runCheckoutActiveBranch } from "../src/project/checkout-active-branch.js";

function run(cmd, { cwd }) {
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("--checkout-active-branch switches canonical clone to repo.active_branch and hard-resets", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-checkout-active-"));
  const projectRoot = join(root, "project");
  const reposBase = join(root, "repos");
  mkdirSync(join(projectRoot, "ai"), { recursive: true });
  mkdirSync(reposBase, { recursive: true });

  const remoteBare = join(root, "remote.git");
  assert.ok(run(`git init --bare -q "${remoteBare}"`, { cwd: root }).ok);

  const seed = join(root, "seed");
  mkdirSync(seed, { recursive: true });
  assert.ok(run("git init -q", { cwd: seed }).ok);
  assert.ok(run('git config user.email "test@example.com"', { cwd: seed }).ok);
  assert.ok(run('git config user.name "test"', { cwd: seed }).ok);
  assert.ok(run("git branch -m main", { cwd: seed }).ok);

  writeFileSync(join(seed, "README.md"), "main\n", "utf8");
  assert.ok(run("git add README.md", { cwd: seed }).ok);
  assert.ok(run('git commit -m "main" -q', { cwd: seed }).ok);
  assert.ok(run(`git remote add origin "${remoteBare}"`, { cwd: seed }).ok);
  assert.ok(run("git push -u origin main -q", { cwd: seed }).ok);

  assert.ok(run("git checkout -b develop -q", { cwd: seed }).ok);
  writeFileSync(join(seed, "README.md"), "develop\n", "utf8");
  assert.ok(run("git add README.md", { cwd: seed }).ok);
  assert.ok(run('git commit -m "develop" -q', { cwd: seed }).ok);
  assert.ok(run("git push -u origin develop -q", { cwd: seed }).ok);

  const canonical = join(reposBase, "RepoA");
  assert.ok(run(`git clone -q "${remoteBare}" "${canonical}"`, { cwd: root }).ok);
  // Ensure we are on main initially.
  assert.ok(run("git -C RepoA switch main -q", { cwd: reposBase }).ok);
  assert.equal(run("git rev-parse --abbrev-ref HEAD", { cwd: canonical }).stdout.trim(), "main");

  const reposJsonPath = join(projectRoot, "ai", "REPOS.json");
  const reposJson = {
    version: 1,
    base_dir: reposBase,
    repos: [{ repo_id: "repo-a", name: "RepoA", path: "RepoA", status: "active", team_id: "FrontendApp", active_branch: "develop" }],
  };
  writeFileSync(reposJsonPath, JSON.stringify(reposJson, null, 2) + "\n", "utf8");

  const result = await runCheckoutActiveBranch({ workRoot: projectRoot, onlyActive: true, dryRun: false });
  assert.ok(result.ok, result.message);
  assert.equal(result.totals.ok, 1);

  assert.equal(run("git rev-parse --abbrev-ref HEAD", { cwd: canonical }).stdout.trim(), "develop");
  assert.equal(readFileSync(join(canonical, "README.md"), "utf8"), "develop\n");

  const reportText = readFileSync(join(result.artifactsDir, "report.json"), "utf8");
  const report = JSON.parse(reportText);
  assert.equal(report.totals.failed, 0);
  assert.equal(report.repos[0].resolved_branch, "develop");

  // Rescan commands: now that develop is checked out, package.json (created below) is detectable and commands are written to REPOS.json.
  writeFileSync(join(canonical, "package.json"), JSON.stringify({ name: "x", version: "1.0.0", scripts: { build: "echo ok" } }, null, 2) + "\n", "utf8");
  assert.ok(run("git add package.json", { cwd: canonical }).ok);
  assert.ok(run('git commit -m "add package.json" -q', { cwd: canonical }).ok);
  assert.ok(run("git push -u origin develop -q", { cwd: canonical }).ok);
  assert.ok(run("git fetch -q origin", { cwd: canonical }).ok);
  assert.ok(run("git reset --hard origin/develop -q", { cwd: canonical }).ok);

  const rescan = await runCheckoutActiveBranch({ workRoot: projectRoot, onlyActive: true, dryRun: false, rescanCommands: true });
  assert.ok(rescan.ok, rescan.message);
  const updated = JSON.parse(readFileSync(reposJsonPath, "utf8"));
  assert.equal(updated.repos[0].repo_id, "repo-a");
  assert.equal(updated.repos[0].commands.package_manager, "npm");
  assert.equal(updated.repos[0].commands.cwd, ".");
  assert.equal(updated.repos[0].commands.build, "npm run build");
});

test("--checkout-active-branch fails when active_branch missing on remote (no guessing)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-checkout-active-missing-"));
  const projectRoot = join(root, "project");
  const reposBase = join(root, "repos");
  mkdirSync(join(projectRoot, "ai"), { recursive: true });
  mkdirSync(reposBase, { recursive: true });

  const remoteBare = join(root, "remote.git");
  assert.ok(run(`git init --bare -q "${remoteBare}"`, { cwd: root }).ok);

  const seed = join(root, "seed");
  mkdirSync(seed, { recursive: true });
  assert.ok(run("git init -q", { cwd: seed }).ok);
  assert.ok(run('git config user.email "test@example.com"', { cwd: seed }).ok);
  assert.ok(run('git config user.name "test"', { cwd: seed }).ok);
  assert.ok(run("git branch -m main", { cwd: seed }).ok);
  writeFileSync(join(seed, "README.md"), "main\n", "utf8");
  assert.ok(run("git add README.md", { cwd: seed }).ok);
  assert.ok(run('git commit -m "main" -q', { cwd: seed }).ok);
  assert.ok(run(`git remote add origin "${remoteBare}"`, { cwd: seed }).ok);
  assert.ok(run("git push -u origin main -q", { cwd: seed }).ok);

  const canonical = join(reposBase, "RepoB");
  assert.ok(run(`git clone -q "${remoteBare}" "${canonical}"`, { cwd: root }).ok);

  const reposJson = {
    version: 1,
    base_dir: reposBase,
    repos: [{ repo_id: "repo-b", name: "RepoB", path: "RepoB", status: "active", team_id: "FrontendApp", active_branch: "develop" }],
  };
  writeFileSync(join(projectRoot, "ai", "REPOS.json"), JSON.stringify(reposJson, null, 2) + "\n", "utf8");

  const result = await runCheckoutActiveBranch({ workRoot: projectRoot, onlyActive: true, dryRun: false });
  assert.equal(result.ok, false);
  assert.equal(result.totals.failed, 1);

  const reportText = readFileSync(join(result.artifactsDir, "report.json"), "utf8");
  const report = JSON.parse(reportText);
  assert.equal(report.repos[0].failure_reason, "remote_branch_missing");
});
