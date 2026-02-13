import test from "node:test";
import assert from "node:assert/strict";

import { validateKnowledgeInterviewerOutput } from "../src/lane_a/knowledge/architect-interviewer.js";

test("knowledge interviewer cannot ask API-stage questions when VISION/REQUIREMENTS are incomplete", () => {
  const raw = {
    scope: "system",
    stage: "API",
    questions: [],
    known_facts: [],
    assumptions: [],
    unknowns: [],
  };

  const sdlcContext = {
    version: 1,
    completion: { vision: false, requirements: false, domain_data: false, api: false, infra: false, ops: false },
  };

  const v = validateKnowledgeInterviewerOutput(raw, { sdlcContext });
  assert.equal(v.ok, false);
  assert.ok(v.errors.some((e) => String(e).includes("SDLC ladder")));
});

test("knowledge interviewer allows VISION-stage when VISION is incomplete", () => {
  const raw = {
    scope: "system",
    stage: "VISION",
    questions: [
      {
        id: "Q_test_vision",
        text: "What is the product vision in one paragraph?",
        why_now: "VISION is missing; this gates all later SDLC questions.",
        blocks: ["REQUIREMENTS", "DOMAIN_DATA", "API", "INFRA", "OPS"],
        evidence_refs: [],
        evidence_missing: ["need intent evidence: file: kickoff inputs.vision (not provided)"],
      },
    ],
    known_facts: [],
    assumptions: [],
    unknowns: [],
  };

  const sdlcContext = {
    version: 1,
    completion: { vision: false, requirements: false, domain_data: false, api: false, infra: false, ops: false },
  };

  const v = validateKnowledgeInterviewerOutput(raw, { sdlcContext });
  assert.equal(v.ok, true, `expected ok=true, got errors: ${(v.errors || []).join(" | ")}`);
});

