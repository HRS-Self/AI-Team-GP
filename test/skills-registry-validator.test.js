import test from "node:test";
import assert from "node:assert/strict";

import { ContractValidationError, validateSkillsRegistry } from "../src/contracts/validators/index.js";

function validRegistry() {
  return {
    version: 1,
    updated_at: "2026-02-14T00:00:00.000Z",
    skills: {
      "repo.audit": {
        skill_id: "repo.audit",
        title: "Repo Audit",
        description: "Audits repository conventions.",
        tags: ["repo", "audit"],
        path: "skills/repo.audit/skill.md",
        status: "active",
      },
    },
  };
}

test("skills registry validator rejects invalid skill_id", () => {
  const bad = validRegistry();
  bad.skills.BAD = {
    skill_id: "BAD",
    title: "bad",
    description: "bad",
    tags: [],
    path: "skills/BAD/skill.md",
    status: "active",
  };
  assert.throws(
    () => validateSkillsRegistry(bad),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.skills['BAD']"),
  );
});

test("skills registry validator rejects bad path", () => {
  const bad = validRegistry();
  bad.skills["repo.audit"].path = "../skills/repo.audit/skill.md";
  assert.throws(
    () => validateSkillsRegistry(bad),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.skills['repo.audit'].path"),
  );
});

test("skills registry validator accepts valid payload", () => {
  const ok = validateSkillsRegistry(validRegistry());
  assert.equal(ok.skills["repo.audit"].status, "active");
});
