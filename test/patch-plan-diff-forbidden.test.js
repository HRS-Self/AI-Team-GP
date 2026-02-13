import test from "node:test";
import assert from "node:assert/strict";

import { validatePatchPlan } from "../src/validators/patch-plan-validator.js";

function basePlan() {
  return {
    version: 1,
    work_id: "W-test",
    repo_id: "repo-test",
    repo_path: "Repo-Test",
    target_branch: { name: "develop", source: "routing", confidence: 1 },
    team_id: "FrontendApp",
    kind: "App",
    is_hexa: false,
    derived_from: { proposal_id: "ai/work/W-test/proposals/x.json", proposal_hash: "abc", proposal_agent_id: "FrontendApp__planner__01", timestamp: "2026-02-03T00:00:00.000Z" },
    intent_summary: "Test plan",
    scope: { allowed_paths: ["README.md"], forbidden_paths: [], allowed_ops: ["edit"] },
    edits: [
      {
        path: "README.md",
        op: "edit",
        rationale: "Test edit",
        patch: "--- a/README.md\n+++ b/README.md\n@@ -1,1 +1,1 @@\n-test\n+test\n",
      },
    ],
    commands: { cwd: ".", package_manager: null, install: null, lint: null, test: null, build: null },
    risk: { level: "low", notes: "" },
    constraints: { no_branch_create: true, requires_training: false, hexa_authoring_mode: null, blockly_compat_required: null },
  };
}

test("patch plan validation fails when edits[i].diff exists (forbidden)", () => {
  const p = basePlan();
  p.edits[0].diff = "diff --git a/README.md b/README.md";
  const v = validatePatchPlan(p);
  assert.equal(v.ok, false);
  assert.ok(v.errors.some((e) => e.includes("edits[0].diff is forbidden")), v.errors.join("\n"));
});

test("patch plan validation fails when edits[i].patch missing/empty", () => {
  const p = basePlan();
  p.edits[0].patch = "   ";
  const v = validatePatchPlan(p);
  assert.equal(v.ok, false);
  assert.ok(v.errors.some((e) => e.includes("edits[0].patch missing/empty")), v.errors.join("\n"));
});

test("patch plan validation passes for patch-only edits", () => {
  const v = validatePatchPlan(basePlan());
  assert.equal(v.ok, true, v.errors.join("\n"));
});
