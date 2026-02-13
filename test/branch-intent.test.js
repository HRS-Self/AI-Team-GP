import test from "node:test";
import assert from "node:assert/strict";

import { extractExplicitTargetBranchFromIntake } from "../src/utils/branch-intent.js";

test("extractExplicitTargetBranchFromIntake: matches develop branch shorthand", () => {
  const r = extractExplicitTargetBranchFromIntake("Update README in develop branch please");
  assert.ok(r);
  assert.equal(r.name, "develop");
  assert.equal(r.source, "explicit");
  assert.equal(r.confidence, 1.0);
  assert.match(r.matched_token, /develop/i);
});

test("extractExplicitTargetBranchFromIntake: matches 'on develop' without word 'branch'", () => {
  const r = extractExplicitTargetBranchFromIntake("Update README on develop and send PR");
  assert.ok(r);
  assert.equal(r.name, "develop");
  assert.match(r.matched_token, /on develop/i);
});

test("extractExplicitTargetBranchFromIntake: normalizes 'dev' to 'develop'", () => {
  const r = extractExplicitTargetBranchFromIntake("Do it against dev");
  assert.ok(r);
  assert.equal(r.name, "develop");
});

test("extractExplicitTargetBranchFromIntake: captures explicit target branch name", () => {
  const r = extractExplicitTargetBranchFromIntake("target branch: feature/ABC-123");
  assert.ok(r);
  assert.equal(r.name, "feature/ABC-123");
  assert.match(r.matched_token, /target branch/i);
});

