import { assertArray, assertEnumString, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

function validateQuestionQuality(q, path) {
  const s = assertNonUuidString(q, path, { minLength: 1 });
  const lower = s.toLowerCase();
  const bannedStarts = ["how does", "how do", "explain", "what is the best", "what's the best", "can you explain"];
  for (const b of bannedStarts) if (lower.startsWith(b)) fail(path, "invalid question (must be a decision, not an explanation)");
  const bannedContains = ["best approach", "best practice", "internals", "implementation details"];
  for (const b of bannedContains) if (lower.includes(b)) fail(path, "invalid question (must not solicit free-form advice)");
  const allowedLead = ["is ", "are ", "should ", "which ", "who ", "what ", "does ", "do ", "will ", "can "];
  const ok = allowedLead.some((p) => lower.startsWith(p));
  if (!ok) fail(path, "invalid question (must start with a bounded decision form like 'Is/Which/Who/Should')");
  return s;
}

export function validateDecisionPacket(data) {
  assertPlainObject(data, "$");
  const allowedTop = new Set([
    "version",
    "type",
    "decision_id",
    "scope",
    "trigger",
    "blocking_state",
    "context",
    "questions",
    "assumptions_if_unanswered",
    "created_at",
    "answered_at",
    "status",
  ]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  if (data.type !== undefined) assertEnumString(data.type, "$.type", ["INVARIANT_WAIVER"]);
  assertNonUuidString(data.decision_id, "$.decision_id", { minLength: 8 });
  const scope = assertNonUuidString(data.scope, "$.scope", { minLength: 1 });
  if (!(scope === "system" || scope.startsWith("repo:"))) fail("$.scope", "must be 'system' or 'repo:<repo_id>'");
  assertEnumString(data.trigger, "$.trigger", ["repo_committee", "integration_committee", "state_machine"]);
  assertNonUuidString(data.blocking_state, "$.blocking_state", { minLength: 1 });

  assertPlainObject(data.context, "$.context");
  const ctxAllowed = new Set(["summary", "why_automation_failed", "what_is_known"]);
  for (const k of Object.keys(data.context)) if (!ctxAllowed.has(k)) fail(`$.context.${k}`, "unknown field");
  assertNonUuidString(data.context.summary, "$.context.summary", { minLength: 1 });
  assertNonUuidString(data.context.why_automation_failed, "$.context.why_automation_failed", { minLength: 1 });
  assertArray(data.context.what_is_known, "$.context.what_is_known");
  for (let i = 0; i < data.context.what_is_known.length; i += 1) assertNonUuidString(data.context.what_is_known[i], `$.context.what_is_known[${i}]`, { minLength: 1 });

  assertArray(data.questions, "$.questions", { minItems: 1 });
  for (let i = 0; i < data.questions.length; i += 1) {
    const q = data.questions[i];
    assertPlainObject(q, `$.questions[${i}]`);
    const allowedQ = new Set(["id", "question", "expected_answer_type", "constraints", "blocks", "answer", "answered_at"]);
    for (const k of Object.keys(q)) if (!allowedQ.has(k)) fail(`$.questions[${i}].${k}`, "unknown field");
    assertNonUuidString(q.id, `$.questions[${i}].id`, { minLength: 8 });
    validateQuestionQuality(q.question, `$.questions[${i}].question`);
    assertEnumString(q.expected_answer_type, `$.questions[${i}].expected_answer_type`, ["text", "choice", "boolean", "reference"]);
    if (typeof q.constraints !== "string") fail(`$.questions[${i}].constraints`, "must be a string");
    assertArray(q.blocks, `$.questions[${i}].blocks`);
    for (let j = 0; j < q.blocks.length; j += 1) assertNonUuidString(q.blocks[j], `$.questions[${i}].blocks[${j}]`, { minLength: 1 });
    if (q.answered_at !== undefined) assertIsoDateTimeZ(q.answered_at, `$.questions[${i}].answered_at`);
  }

  assertNonUuidString(data.assumptions_if_unanswered, "$.assumptions_if_unanswered", { minLength: 1 });
  assertIsoDateTimeZ(data.created_at, "$.created_at");
  if (data.answered_at !== undefined) assertIsoDateTimeZ(data.answered_at, "$.answered_at");
  const status = assertEnumString(data.status, "$.status", ["open", "answered"]);
  if (status === "answered") {
    if (data.answered_at === undefined) fail("$.answered_at", "required when status is answered");
    for (let i = 0; i < data.questions.length; i += 1) {
      const q = data.questions[i];
      if (q.answer === undefined) fail(`$.questions[${i}].answer`, "required when status is answered");
      if (q.answered_at === undefined) fail(`$.questions[${i}].answered_at`, "required when status is answered");
    }
  }

  return data;
}
