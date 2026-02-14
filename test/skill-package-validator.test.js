import test from "node:test";
import assert from "node:assert/strict";

import { ContractValidationError, validateSkillPackage } from "../src/contracts/validators/index.js";
import { sha256Hex } from "../src/utils/fs-hash.js";

function validSkillMd() {
  return [
    "# Overview",
    "Reusable guardrails for API boundary behavior.",
    "",
    "# When to use",
    "- Use for API contract updates.",
    "",
    "# When NOT to use",
    "- Do not use for unrelated UI copy-only changes.",
    "",
    "# Constraints",
    "- Keep response shape backward compatible.",
    "",
    "# Known failure modes",
    "- Missing regression tests for changed response schemas.",
    "",
  ].join("\n");
}

function validMetadata(skillMd) {
  return {
    version: 1,
    skill_id: "api-contract-guardrails",
    title: "API Contract Guardrails",
    domain: "api-contracts",
    applies_to: ["repo:backend-api"],
    created_at: "2026-02-14T00:00:00.000Z",
    updated_at: "2026-02-14T00:00:00.000Z",
    hash: sha256Hex(skillMd),
    evidence_refs: ["EVID_12345678"],
    source_scope: "repo:backend-api",
    dependencies: [],
    author: "skill.author",
  };
}

test("skill package validator accepts valid package", () => {
  const skillMd = validSkillMd();
  const metadata = validMetadata(skillMd);
  const out = validateSkillPackage(metadata, { skillMd });
  assert.equal(out.skill_id, "api-contract-guardrails");
});

test("skill package validator rejects extra key", () => {
  const skillMd = validSkillMd();
  const metadata = validMetadata(skillMd);
  metadata.unexpected = true;
  assert.throws(
    () => validateSkillPackage(metadata, { skillMd }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.unexpected"),
  );
});

test("skill package validator rejects hash mismatch", () => {
  const skillMd = validSkillMd();
  const metadata = validMetadata(skillMd);
  metadata.hash = "a".repeat(64);
  assert.throws(
    () => validateSkillPackage(metadata, { skillMd }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.hash"),
  );
});

test("skill package validator rejects oversized skill.md", () => {
  const skillMd = "A".repeat(81 * 1024);
  const metadata = validMetadata(skillMd);
  assert.throws(
    () => validateSkillPackage(metadata, { skillMd }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$skill_md"),
  );
});

