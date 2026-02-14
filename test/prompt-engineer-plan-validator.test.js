import test from "node:test";
import assert from "node:assert/strict";

import { ContractValidationError, validatePromptEngineerPlan } from "../src/contracts/validators/index.js";

function makeValidPlan() {
  return {
    version: 1,
    role: "prompt_engineer",
    created_at: "2026-02-14T00:00:00.000Z",
    scope: "repo:repo-a",
    decision: {
      skills_to_load: ["skill.alpha"],
      skills_missing: [],
      reasoning_style: "strict",
      risk: "normal",
    },
    prompt_delta: {
      system_append: "Add strict output schema reminder.",
      developer_append: "",
      user_append: "",
      forbidden_inclusions: ["no secrets", "no absolute paths"],
    },
    notes: [{ type: "info", text: "Conservative additive delta only." }],
  };
}

test("prompt engineer plan validator accepts valid plan", () => {
  const valid = makeValidPlan();
  const out = validatePromptEngineerPlan(valid);
  assert.equal(out.role, "prompt_engineer");
});

test("prompt engineer plan validator rejects extra key", () => {
  const bad = { ...makeValidPlan(), extra: true };
  assert.throws(
    () => validatePromptEngineerPlan(bad),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.extra"),
  );
});

test("prompt engineer plan validator rejects bad enum", () => {
  const bad = makeValidPlan();
  bad.decision.reasoning_style = "creative";
  assert.throws(
    () => validatePromptEngineerPlan(bad),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.decision.reasoning_style"),
  );
});

test("prompt engineer plan validator rejects created_at without Z", () => {
  const bad = makeValidPlan();
  bad.created_at = "2026-02-14T00:00:00.000+00:00";
  assert.throws(
    () => validatePromptEngineerPlan(bad),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.created_at"),
  );
});
