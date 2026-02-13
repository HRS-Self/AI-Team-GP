import test from "node:test";
import assert from "node:assert/strict";

import { validateKnowledgeGap } from "../src/validators/knowledge-gap-validator.js";

test("knowledge gap validator rejects missing required fields", () => {
  const v = validateKnowledgeGap({ scope: "system" });
  assert.equal(v.ok, false);
  assert.ok(v.errors.length > 0);
});

test("knowledge gap validator normalizes and assigns stable id", () => {
  const v = validateKnowledgeGap({
    scope: "system",
    category: "integration_missing",
    severity: "high",
    risk: "high",
    summary: "Missing endpoint",
    expected: "Endpoint exists",
    observed: "No endpoint",
    evidence: [{ type: "endpoint", method: "GET", path: "/api/x" }],
    suggested_intake: { repo_id: "repo-a", title: "Fix", body: "Do it", labels: ["gap", "ai"] },
  });
  assert.equal(v.ok, true);
  assert.ok(v.normalized.gap_id.startsWith("GAP_"));
  assert.equal(v.normalized.scope, "system");
});

test("knowledge gap validator rejects missing suggested_intake (no auto-fill)", () => {
  const v = validateKnowledgeGap({
    scope: "repo:repo-a",
    category: "feature_missing",
    severity: "high",
    risk: "high",
    summary: "Thing missing",
    expected: "Thing exists",
    observed: "Thing not found",
    evidence: [{ type: "file", path: "README.md", hint: "missing mention" }],
  });
  assert.equal(v.ok, false);
  assert.ok(v.errors.join(" | ").includes("suggested_intake is required"));
});

test("knowledge gap validator rejects missing evidence", () => {
  const v = validateKnowledgeGap({
    scope: "system",
    category: "integration_missing",
    severity: "high",
    risk: "high",
    summary: "Missing endpoint",
    expected: "Endpoint exists",
    observed: "No endpoint",
    evidence: [],
    suggested_intake: { repo_id: "repo-a", title: "Fix", body: "Do it", labels: ["gap", "ai"] },
  });
  assert.equal(v.ok, false);
  assert.ok(v.errors.join(" | ").includes("evidence is required"));
});
