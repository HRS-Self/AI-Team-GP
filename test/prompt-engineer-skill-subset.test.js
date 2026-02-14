import test from "node:test";
import assert from "node:assert/strict";

import { runPromptEngineer } from "../src/prompt_engineer/prompt-engineer.js";

function inputWithCandidates() {
  return {
    projectRoot: "/opt/AI-Projects/demo/ops",
    scope: "repo:repo-a",
    base_prompt: "Do task",
    base_system: "System",
    context: {},
    constraints: {},
    knowledge_snippets: [],
    candidate_skills: [{ skill_id: "repo.audit", title: "Repo Audit", description: "x", tags: [] }],
    profiles: {
      profiles: {
        "prompt.engineer": {
          provider: "openai",
          model: "stub-model",
        },
      },
    },
  };
}

test("runPromptEngineer rejects skills_to_load outside candidate skill set", async () => {
  const input = inputWithCandidates();
  input.env = {
    systemPromptText: "JSON only",
    createLlmClient: () => ({ ok: true, llm: { stub: true } }),
    createDeepAgent: () => ({
      invoke: async () => ({
        output_text: JSON.stringify({
          version: 1,
          role: "prompt_engineer",
          created_at: "2026-02-14T00:00:00.000Z",
          scope: "repo:repo-a",
          decision: {
            skills_to_load: ["unknown.skill"],
            skills_missing: [],
            reasoning_style: "strict",
            risk: "normal",
          },
          prompt_delta: {
            system_append: "",
            developer_append: "",
            user_append: "",
            forbidden_inclusions: [],
          },
          notes: [],
        }),
      }),
    }),
  };

  await assert.rejects(runPromptEngineer(input), /selected disallowed skill_id/i);
});
