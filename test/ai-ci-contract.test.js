import test from "node:test";
import assert from "node:assert/strict";

import { buildAiCiContractFromPatchPlan } from "../src/lane_b/ci/ai-ci-contract.js";

test("ai-ci contract: happy path (omit nulls, preserve strings)", () => {
  const plan = {
    version: 1,
    commands: {
      cwd: ".",
      package_manager: "npm",
      install: "npm ci",
      lint: "npm run lint",
      build: "npm run build",
      test: null,
    },
  };
  const res = buildAiCiContractFromPatchPlan({ patchPlanJson: plan });
  assert.equal(res.ok, true);
  assert.deepEqual(res.contract, { version: 1, install: "npm ci", lint: "npm run lint", build: "npm run build" });
  assert.ok(!res.text.includes("\"test\""));
});

test("ai-ci contract: includes test when non-null", () => {
  const plan = { version: 1, commands: { install: "x", lint: null, build: null, test: "npm test" } };
  const res = buildAiCiContractFromPatchPlan({ patchPlanJson: plan });
  assert.equal(res.ok, true);
  assert.deepEqual(res.contract, { version: 1, install: "x", test: "npm test" });
});

test("ai-ci contract: fails when commands missing", () => {
  const plan = { version: 1 };
  const res = buildAiCiContractFromPatchPlan({ patchPlanJson: plan });
  assert.equal(res.ok, false);
  assert.match(res.message, /missing required top-level key: commands/i);
});

test("ai-ci contract: fails when all commands null", () => {
  const plan = { version: 1, commands: { install: null, lint: null, build: null, test: null } };
  const res = buildAiCiContractFromPatchPlan({ patchPlanJson: plan });
  assert.equal(res.ok, false);
  assert.match(res.message, /all commands are null/i);
});
