import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

test("AI-only CI workflow template is label-gated and uses TestRunners runner group", () => {
  const p = resolve(process.cwd(), "src", "templates", "workflows", "ai-team-ci.yml");
  const yml = readFileSync(p, "utf8");

  assert.match(yml, /name:\s*AI-Team CI/i);
  assert.match(yml, /pull_request:/);
  assert.match(yml, /types:\s*\[opened,\s*synchronize,\s*reopened,\s*labeled\]/);

  assert.match(yml, /contains\(\s*github\.event\.pull_request\.labels\.\*\.\s*name\s*,\s*'ai-team'\s*\)/);

  assert.match(yml, /runs-on:\s*\n\s*group:\s*TestRunners/);
  assert.ok(!/deploy/i.test(yml), "workflow must not contain deploy steps");
});

