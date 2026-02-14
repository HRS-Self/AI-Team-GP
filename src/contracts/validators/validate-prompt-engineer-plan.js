import { fail } from "./error.js";
import { assertArray, assertEnumString, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";

function assertScope(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 1 });
  if (s !== "system" && !/^repo:[A-Za-z0-9._-]+$/.test(s)) fail(path, "must be 'system' or 'repo:<repo_id>'");
  return s;
}

function assertString(value, path) {
  if (typeof value !== "string") fail(path, "must be a string");
  return value;
}

function assertStringArray(arr, path) {
  assertArray(arr, path);
  for (let i = 0; i < arr.length; i += 1) assertString(arr[i], `${path}[${i}]`);
}

export function validatePromptEngineerPlan(data) {
  assertPlainObject(data, "$");

  const allowedTop = new Set(["version", "role", "created_at", "scope", "decision", "prompt_delta", "notes"]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  if (data.role !== "prompt_engineer") fail("$.role", "must be prompt_engineer");
  assertIsoDateTimeZ(data.created_at, "$.created_at");
  assertScope(data.scope, "$.scope");

  assertPlainObject(data.decision, "$.decision");
  {
    const d = data.decision;
    const allowed = new Set(["skills_to_load", "skills_missing", "reasoning_style", "risk"]);
    for (const k of Object.keys(d)) if (!allowed.has(k)) fail(`$.decision.${k}`, "unknown field");
    assertStringArray(d.skills_to_load, "$.decision.skills_to_load");
    assertStringArray(d.skills_missing, "$.decision.skills_missing");
    assertEnumString(d.reasoning_style, "$.decision.reasoning_style", ["strict", "balanced"]);
    assertEnumString(d.risk, "$.decision.risk", ["low", "normal", "high", "critical", "unknown"]);
  }

  assertPlainObject(data.prompt_delta, "$.prompt_delta");
  {
    const p = data.prompt_delta;
    const allowed = new Set(["system_append", "developer_append", "user_append", "forbidden_inclusions"]);
    for (const k of Object.keys(p)) if (!allowed.has(k)) fail(`$.prompt_delta.${k}`, "unknown field");
    assertString(p.system_append, "$.prompt_delta.system_append");
    assertString(p.developer_append, "$.prompt_delta.developer_append");
    assertString(p.user_append, "$.prompt_delta.user_append");
    assertStringArray(p.forbidden_inclusions, "$.prompt_delta.forbidden_inclusions");
  }

  assertArray(data.notes, "$.notes");
  for (let i = 0; i < data.notes.length; i += 1) {
    const n = data.notes[i];
    assertPlainObject(n, `$.notes[${i}]`);
    const allowed = new Set(["type", "text"]);
    for (const k of Object.keys(n)) if (!allowed.has(k)) fail(`$.notes[${i}].${k}`, "unknown field");
    assertEnumString(n.type, `$.notes[${i}].type`, ["warning", "info", "blocker"]);
    assertString(n.text, `$.notes[${i}].text`);
  }

  return data;
}
