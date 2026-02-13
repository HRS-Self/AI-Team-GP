import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { checkoutBranchDeterministic, currentBranchName } from "../src/utils/git-checkout.js";

function run(cmd, { cwd }) {
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("checkoutBranchDeterministic switches from main to develop when clean", () => {
  const dir = mkdtempSync(join(tmpdir(), "ai-team-checkout-"));
  assert.ok(run("git init -q", { cwd: dir }).ok);
  assert.ok(run('git config user.email "test@example.com"', { cwd: dir }).ok);
  assert.ok(run('git config user.name "test"', { cwd: dir }).ok);
  assert.ok(run("git branch -m main", { cwd: dir }).ok);

  writeFileSync(join(dir, "README.md"), "main\n", "utf8");
  assert.ok(run("git add README.md", { cwd: dir }).ok);
  assert.ok(run('git commit -m "main" -q', { cwd: dir }).ok);

  assert.ok(run("git checkout -b develop -q", { cwd: dir }).ok);
  writeFileSync(join(dir, "README.md"), "develop\n", "utf8");
  assert.ok(run("git add README.md", { cwd: dir }).ok);
  assert.ok(run('git commit -m "develop" -q', { cwd: dir }).ok);

  assert.ok(run("git checkout main -q", { cwd: dir }).ok);
  assert.equal(currentBranchName(dir), "main");

  const res = checkoutBranchDeterministic(dir, "develop");
  assert.ok(res.ok);
  assert.equal(res.changed, true);
  assert.equal(currentBranchName(dir), "develop");
});

