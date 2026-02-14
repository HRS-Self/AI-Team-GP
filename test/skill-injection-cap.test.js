import test from "node:test";
import assert from "node:assert/strict";

import { maybeAugmentPromptWithEngineer } from "../src/llm/prompt-augment.js";

function buildPlan() {
  return {
    version: 1,
    role: "prompt_engineer",
    created_at: "2026-02-14T00:00:00.000Z",
    scope: "system",
    decision: {
      skills_to_load: ["skill.one", "skill.two", "skill.three"],
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
  };
}

test("skill injection enforces byte cap and records warning note", async () => {
  const plan = buildPlan();
  const bigA = "A".repeat(70 * 1024);
  const bigB = "B".repeat(70 * 1024);
  const bigC = "C".repeat(40 * 1024);

  const result = await maybeAugmentPromptWithEngineer({
    enabled: true,
    enableSkills: true,
    projectRoot: "/opt/AI-Projects/demo/ops",
    baseMessages: [
      { role: "system", content: "Base system" },
      { role: "user", content: "Base user" },
    ],
    input: {
      scope: "system",
      candidate_skills: [
        { skill_id: "skill.one", title: "one", description: "", tags: [] },
        { skill_id: "skill.two", title: "two", description: "", tags: [] },
        { skill_id: "skill.three", title: "three", description: "", tags: [] },
      ],
    },
    runPromptEngineerImpl: async () => plan,
    resolveAllowedSkillContentsImpl: async () => [
      { skill_id: "skill.one", sha256: "1".repeat(64), content: bigA, pinned: false },
      { skill_id: "skill.two", sha256: "2".repeat(64), content: bigB, pinned: false },
      { skill_id: "skill.three", sha256: "3".repeat(64), content: bigC, pinned: false },
    ],
  });

  const systemMessages = result.messages.filter((m) => m && m.role === "system").map((m) => String(m.content || ""));
  const injected = systemMessages.find((content) => content.includes("=== BEGIN SKILL:"));
  assert.ok(injected);
  assert.equal(injected.includes("=== BEGIN SKILL: skill.one"), true);
  assert.equal(injected.includes("=== BEGIN SKILL: skill.two"), true);
  assert.equal(injected.includes("=== BEGIN SKILL: skill.three"), false);
  assert.equal(result.messages.some((m) => m && m.role === "user" && String(m.content || "").includes("BEGIN SKILL")), false);
  assert.equal(Array.isArray(result.plan.notes), true);
  assert.equal(result.plan.notes.some((n) => n && n.type === "warning" && String(n.text || "").includes("Skill injection cap reached")), true);
});
