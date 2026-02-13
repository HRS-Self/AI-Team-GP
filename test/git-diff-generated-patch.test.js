import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

function run(cmd, { cwd }) {
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function parseFirstHunkHeader(patchText) {
  const lines = String(patchText || "").split("\n");
  const headerLine = lines.find((l) => l.startsWith("@@ "));
  if (!headerLine) return null;
  const m = headerLine.match(/^@@\s+\-(\d+)(?:,(\d+))?\s+\+(\d+)(?:,(\d+))?\s+@@/);
  if (!m) return null;
  const oldStart = Number(m[1]);
  const oldCount = m[2] ? Number(m[2]) : 1;
  const newStart = Number(m[3]);
  const newCount = m[4] ? Number(m[4]) : 1;
  return { oldStart, oldCount, newStart, newCount, headerLine };
}

test("generated patch via git diff has correct hunks and applies cleanly", () => {
  const dir = mkdtempSync(join(tmpdir(), "ai-team-gitpatch-"));
  assert.ok(run("git init -q", { cwd: dir }).ok);
  assert.ok(run('git config user.email "test@example.com"', { cwd: dir }).ok);
  assert.ok(run('git config user.name "test"', { cwd: dir }).ok);

  writeFileSync(join(dir, "README.md"), "hello\n", "utf8");
  assert.ok(run("git add README.md", { cwd: dir }).ok);
  assert.ok(run('git commit -m "init" -q', { cwd: dir }).ok);

  // Intentionally broken hunk header counts (common LLM failure mode).
  const rawPatch = [
    "diff --git a/README.md b/README.md",
    "index 0000000..1111111 100644",
    "--- a/README.md",
    "+++ b/README.md",
    "@@ -1,3 +1,4 @@",
    " hello",
    "+world",
    "",
  ].join("\n");
  writeFileSync(join(dir, "raw.patch"), rawPatch, "utf8");

  // Apply with recount, generate a clean patch using git diff, then reset and apply the clean patch.
  assert.ok(run("git apply --recount --check raw.patch", { cwd: dir }).ok);
  assert.ok(run("git apply --recount raw.patch", { cwd: dir }).ok);

  const gen = run('git diff --no-ext-diff -- "README.md"', { cwd: dir });
  assert.ok(gen.ok);
  writeFileSync(join(dir, "gen.patch"), gen.stdout, "utf8");

  const h = parseFirstHunkHeader(gen.stdout);
  assert.ok(h, "expected a unified diff hunk header");
  assert.equal(h.oldStart, 1);
  assert.equal(h.oldCount, 1);
  assert.equal(h.newStart, 1);
  assert.equal(h.newCount, 2);

  assert.ok(run("git reset --hard -q", { cwd: dir }).ok);
  assert.ok(run("git apply --check gen.patch", { cwd: dir }).ok);
  assert.ok(run("git apply gen.patch", { cwd: dir }).ok);
  assert.equal(run("cat README.md", { cwd: dir }).stdout, "hello\nworld\n");
});

