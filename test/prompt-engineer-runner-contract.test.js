import test from "node:test";
import assert from "node:assert/strict";

import { runPromptEngineer } from "../src/prompt_engineer/prompt-engineer.js";

function makeRunInput(overrides = {}) {
  return {
    projectRoot: "/opt/AI-Projects/demo/ops",
    scope: "repo:repo-a",
    base_prompt: "Base prompt",
    base_system: "Base system",
    context: { workId: "W-1" },
    constraints: { json_only: true },
    knowledge_snippets: ["snippet-a"],
    candidate_skills: ["skill.a"],
    profiles: {
      profiles: {
        "prompt.engineer": {
          provider: "openai",
          model: "stub-model",
        },
      },
    },
    env: {
      systemPromptText: "JSON only.",
      createLlmClient: () => ({ ok: true, llm: { stub: true } }),
      createDeepAgent: () => ({
        invoke: async () => ({ output_text: "{}" }),
      }),
    },
    ...overrides,
  };
}

function validPlanText(extra = {}) {
  return JSON.stringify({
    version: 1,
    role: "prompt_engineer",
    created_at: "2026-02-14T00:00:00.000Z",
    scope: "repo:repo-a",
    decision: {
      skills_to_load: [],
      skills_missing: [],
      reasoning_style: "strict",
      risk: "normal",
    },
    prompt_delta: {
      system_append: "Append constraints",
      developer_append: "",
      user_append: "",
      forbidden_inclusions: ["no secrets"],
    },
    notes: [{ type: "info", text: "Deterministic plan." }],
    ...extra,
  });
}

test("runPromptEngineer rejects non-JSON output", async () => {
  let createLlmClientCalled = false;
  const input = makeRunInput({
    env: {
      systemPromptText: "JSON only.",
      createLlmClient: () => {
        createLlmClientCalled = true;
        return { ok: true, llm: { stub: true } };
      },
      createDeepAgent: () => ({
        invoke: async () => ({ output_text: "not-json" }),
      }),
    },
  });

  await assert.rejects(runPromptEngineer(input), /must return JSON object only|not valid JSON/i);
  assert.equal(createLlmClientCalled, true);
});

test("runPromptEngineer rejects JSON with extra keys", async () => {
  const input = makeRunInput({
    env: {
      systemPromptText: "JSON only.",
      createLlmClient: () => ({ ok: true, llm: { stub: true } }),
      createDeepAgent: () => ({
        invoke: async () => ({ output_text: validPlanText({ extra_top_level: true }) }),
      }),
    },
  });

  await assert.rejects(runPromptEngineer(input), /\$\.extra_top_level|unknown field/i);
});

test("runPromptEngineer returns validated plan for valid JSON", async () => {
  const input = makeRunInput({
    env: {
      systemPromptText: "JSON only.",
      createLlmClient: () => ({ ok: true, llm: { stub: true } }),
      createDeepAgent: () => ({
        invoke: async () => ({ output_text: validPlanText() }),
      }),
    },
  });

  const plan = await runPromptEngineer(input);
  assert.equal(plan.role, "prompt_engineer");
  assert.equal(plan.decision.reasoning_style, "strict");
});
