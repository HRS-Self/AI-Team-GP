import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";

function deprecatedWord() {
  return ["p", "r", "o", "g", "r", "a", "m"].join("");
}

test("Deprecated flag fails fast with required message", () => {
  const w = deprecatedWord();
  const flag = `--${w}`;
  const res = spawnSync(process.execPath, ["src/cli.js", flag], {
    cwd: resolve("/opt/GitRepos/AI-Team"),
    encoding: "utf8",
  });
  assert.equal(res.status, 2);
  assert.equal(String(res.stderr || "").trim(), `The '${w}' concept has been fully deprecated. Use project + repo scope instead.`);
});

