import test from "node:test";
import assert from "node:assert/strict";

import { maybeAugmentPromptWithEngineer } from "../src/llm/prompt-augment.js";

test("maybeAugmentPromptWithEngineer leaves messages unchanged when disabled", async () => {
  const baseMessages = [{ role: "system", content: "Base system" }, { role: "user", content: "Base user" }];
  const out = await maybeAugmentPromptWithEngineer({ enabled: false, baseMessages, input: { scope: "system" } });

  assert.deepEqual(out.messages, baseMessages);
  assert.equal(out.plan, null);
});

test("maybeAugmentPromptWithEngineer appends prompt deltas when enabled", async () => {
  const baseMessages = [{ role: "system", content: "Base system" }];
  const plan = {
    version: 1,
    role: "prompt_engineer",
    created_at: "2026-02-14T00:00:00.000Z",
    scope: "system",
    decision: {
      skills_to_load: [],
      skills_missing: [],
      reasoning_style: "balanced",
      risk: "low",
    },
    prompt_delta: {
      system_append: "System delta",
      developer_append: "Developer delta",
      user_append: "User delta",
      forbidden_inclusions: ["no secrets"],
    },
    notes: [],
  };

  const out = await maybeAugmentPromptWithEngineer({
    enabled: true,
    baseMessages,
    input: { scope: "system" },
    runPromptEngineerImpl: async () => plan,
  });

  assert.equal(out.plan.role, "prompt_engineer");
  assert.equal(out.messages.length, 4);
  assert.deepEqual(out.messages[1], { role: "system", content: "System delta" });
  assert.deepEqual(out.messages[2], { role: "developer", content: "Developer delta" });
  assert.deepEqual(out.messages[3], { role: "user", content: "User delta" });
});
