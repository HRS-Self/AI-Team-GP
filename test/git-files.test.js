import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { resolveGitRefForBranch, gitShowFileAtRef } from "../src/utils/git-files.js";

function run(cmd, { cwd }) {
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("git show reads file content from target branch even if current branch differs", () => {
  const dir = mkdtempSync(join(tmpdir(), "ai-team-gitfiles-"));
  assert.ok(run("git init -q", { cwd: dir }).ok);
  assert.ok(run('git config user.email "test@example.com"', { cwd: dir }).ok);
  assert.ok(run('git config user.name "test"', { cwd: dir }).ok);
  // Make default branch deterministic for this test.
  assert.ok(run("git branch -m main", { cwd: dir }).ok);

  writeFileSync(join(dir, "README.md"), "main-branch\n", "utf8");
  assert.ok(run("git add README.md", { cwd: dir }).ok);
  assert.ok(run('git commit -m "main" -q', { cwd: dir }).ok);

  assert.ok(run("git checkout -b develop -q", { cwd: dir }).ok);
  writeFileSync(join(dir, "README.md"), "develop-branch\n", "utf8");
  assert.ok(run("git add README.md", { cwd: dir }).ok);
  assert.ok(run('git commit -m "develop" -q', { cwd: dir }).ok);

  assert.ok(run("git checkout main -q", { cwd: dir }).ok);

  const ref = resolveGitRefForBranch(dir, "develop");
  assert.equal(ref, "develop");

  const shown = gitShowFileAtRef(dir, ref, "README.md");
  assert.ok(shown.ok);
  assert.equal(shown.content, "develop-branch\n");
});
