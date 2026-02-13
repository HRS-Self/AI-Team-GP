import { assertEnumString, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

function normalizeScope(scope) {
  const s = String(scope || "").trim();
  if (!s) return s;
  if (s === "system") return "system";
  if (s.startsWith("repo:")) return `repo:${s.slice("repo:".length).trim()}`;
  return s;
}

export function validateChangeRequest(data) {
  assertPlainObject(data, "$");
  const allowed = new Set(["version", "id", "type", "scope", "title", "body", "severity", "created_at", "status", "linked_meeting_id"]);
  for (const k of Object.keys(data)) if (!allowed.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  assertNonUuidString(data.id, "$.id", { minLength: 4 });
  assertEnumString(data.type, "$.type", ["bug", "feature", "question"]);
  const scope = assertNonUuidString(data.scope, "$.scope", { minLength: 1 });
  const scopeNorm = normalizeScope(scope);
  if (!(scopeNorm === "system" || scopeNorm.startsWith("repo:"))) fail("$.scope", "must be 'system' or 'repo:<id>'");
  data.scope = scopeNorm;
  assertNonUuidString(data.title, "$.title", { minLength: 1 });
  assertNonUuidString(data.body, "$.body", { minLength: 1 });
  assertEnumString(data.severity, "$.severity", ["low", "medium", "high"]);
  assertIsoDateTimeZ(data.created_at, "$.created_at");
  assertEnumString(data.status, "$.status", ["open", "in_meeting", "processed", "rejected"]);
  if (data.linked_meeting_id !== null) assertNonUuidString(data.linked_meeting_id, "$.linked_meeting_id", { minLength: 3 });
  return data;
}

