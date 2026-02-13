import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

function runCli(args, { env }) {
  const res = spawnSync("node", ["src/cli.js", ...args], {
    cwd: "/opt/GitRepos/AI-Team",
    env: { ...process.env, ...(env || {}) },
    encoding: "utf8",
  });
  return { status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("Deprecated approval/gate flags emit deprecation warnings", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-cli-depr-"));
  const opsRoot = join(root, "ops");
  mkdirSync(opsRoot, { recursive: true });

  const a = runCli(["--gate-a"], { env: { AI_PROJECT_ROOT: opsRoot } });
  assert.equal(a.status, 2);
  assert.match(a.stderr, /DEPRECATED:/);
  assert.match(a.stderr, /--apply-approval/);

  const b = runCli(["--approval"], { env: { AI_PROJECT_ROOT: opsRoot } });
  assert.equal(b.status, 2);
  assert.match(b.stderr, /DEPRECATED:/);
  assert.match(b.stderr, /--plan-approval/);
});

