import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { classifyGitApplyCheck } from "../src/utils/git-apply-check.js";

function run(cmd, { cwd }) {
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("classifyGitApplyCheck: detects already-applied patch via reverse --check", () => {
  const dir = mkdtempSync(join(tmpdir(), "ai-team-applycheck-"));
  assert.ok(run("git init -q", { cwd: dir }).ok);
  assert.ok(run('git config user.email "test@example.com"', { cwd: dir }).ok);
  assert.ok(run('git config user.name "test"', { cwd: dir }).ok);

  writeFileSync(join(dir, "README.md"), "hello\n", "utf8");
  assert.ok(run("git add README.md", { cwd: dir }).ok);
  assert.ok(run('git commit -m "init" -q', { cwd: dir }).ok);

  const patch = [
    "diff --git a/README.md b/README.md",
    "index 0000000..1111111 100644",
    "--- a/README.md",
    "+++ b/README.md",
    "@@ -1,1 +1,2 @@",
    " hello",
    "+world",
    "",
  ].join("\n");
  const patchPath = join(dir, "p.patch");
  writeFileSync(patchPath, patch, "utf8");

  // Applies initially.
  const first = classifyGitApplyCheck({ cwd: dir, patchFileAbs: patchPath, recount: false });
  assert.equal(first.ok, true);
  assert.equal(first.status, "applies");
  assert.ok(run("git apply p.patch", { cwd: dir }).ok);

  // Forward check fails, but reverse check succeeds => already_applied.
  const second = classifyGitApplyCheck({ cwd: dir, patchFileAbs: patchPath, recount: false });
  assert.equal(second.ok, true);
  assert.equal(second.status, "already_applied");
});

