import test from "node:test";
import assert from "node:assert/strict";

import { auditQaObligationsAgainstEditPaths } from "../src/lane_b/qa/qa-obligations-audit.js";

test("QA obligations audit blocks when required tests are missing", () => {
  const audit = auditQaObligationsAgainstEditPaths({
    obligations: { must_add_unit: true, must_add_integration: false, must_add_e2e: false },
    editPaths: ["src/index.js"],
    qaApprovalStatus: "pending",
    qaApprovalNotes: null,
  });
  assert.equal(audit.ok, false);
  assert.deepEqual(audit.missing, ["unit"]);
});

test("QA obligations audit allows explicit waiver via QA approval notes", () => {
  const audit = auditQaObligationsAgainstEditPaths({
    obligations: { must_add_unit: true, must_add_integration: false, must_add_e2e: false },
    editPaths: ["src/index.js"],
    qaApprovalStatus: "approved",
    qaApprovalNotes: "waive: unit",
  });
  assert.equal(audit.ok, true);
  assert.deepEqual(audit.missing, []);
});

test("QA obligations audit blocks when QA status is rejected", () => {
  const audit = auditQaObligationsAgainstEditPaths({
    obligations: { must_add_unit: false, must_add_integration: false, must_add_e2e: false },
    editPaths: ["test/foo.test.js"],
    qaApprovalStatus: "rejected",
    qaApprovalNotes: null,
  });
  assert.equal(audit.ok, false);
  assert.ok(audit.missing.includes("qa_rejected"));
});

