import { assertArray, assertEnumString, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

function assertAbsPath(p, path) {
  const s = assertNonUuidString(p, path, { minLength: 1 });
  if (!s.startsWith("/")) fail(path, "must be an absolute path");
  return s;
}

function assertNonNegativeInt(x, path) {
  if (!Number.isFinite(Number(x))) fail(path, "must be a number");
  const n = Math.floor(Number(x));
  if (n < 0) fail(path, "must be >= 0");
  return n;
}

function normalizeScope(scope) {
  const s = String(scope || "").trim();
  if (!s) return s;
  if (s === "system") return "system";
  if (s.startsWith("repo:")) return `repo:${s.slice("repo:".length).trim()}`;
  return s;
}

export function validateMeeting(data) {
  assertPlainObject(data, "$");
  const allowed = new Set([
    "version",
    "meeting_id",
    "project_root",
    "scope",
    "status",
    "knowledge_version_target",
    "inputs",
    "question_cursor",
    "asked_count",
    "answered_count",
    "created_at",
    "updated_at",
    "closed_at",
    "closed_decision",
  ]);
  for (const k of Object.keys(data)) if (!allowed.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  assertNonUuidString(data.meeting_id, "$.meeting_id", { minLength: 3 });
  assertAbsPath(data.project_root, "$.project_root");
  const scope = assertNonUuidString(data.scope, "$.scope", { minLength: 1 });
  const scopeNorm = normalizeScope(scope);
  if (!(scopeNorm === "system" || scopeNorm.startsWith("repo:"))) fail("$.scope", "must be 'system' or 'repo:<id>'");
  data.scope = scopeNorm;

  assertEnumString(data.status, "$.status", ["open", "waiting_for_answer", "ready_to_close", "closed"]);
  assertNonUuidString(data.knowledge_version_target, "$.knowledge_version_target", { minLength: 2 });

  assertPlainObject(data.inputs, "$.inputs");
  const inputsAllowed = new Set(["coverage_path", "sufficiency_path", "committee_status_path", "open_decisions", "integration_gaps", "staleness"]);
  for (const k of Object.keys(data.inputs)) if (!inputsAllowed.has(k)) fail(`$.inputs.${k}`, "unknown field");

  // Paths are informational but must be absolute for operator usability.
  assertAbsPath(data.inputs.coverage_path, "$.inputs.coverage_path");
  assertAbsPath(data.inputs.sufficiency_path, "$.inputs.sufficiency_path");
  assertAbsPath(data.inputs.committee_status_path, "$.inputs.committee_status_path");

  assertArray(data.inputs.open_decisions, "$.inputs.open_decisions");
  for (let i = 0; i < data.inputs.open_decisions.length; i += 1) assertNonUuidString(data.inputs.open_decisions[i], `$.inputs.open_decisions[${i}]`, { minLength: 1 });
  assertArray(data.inputs.integration_gaps, "$.inputs.integration_gaps");
  for (let i = 0; i < data.inputs.integration_gaps.length; i += 1) assertNonUuidString(data.inputs.integration_gaps[i], `$.inputs.integration_gaps[${i}]`, { minLength: 1 });

  assertPlainObject(data.inputs.staleness, "$.inputs.staleness");
  const stAllowed = new Set(["stale", "reasons"]);
  for (const k of Object.keys(data.inputs.staleness)) if (!stAllowed.has(k)) fail(`$.inputs.staleness.${k}`, "unknown field");
  if (typeof data.inputs.staleness.stale !== "boolean") fail("$.inputs.staleness.stale", "must be boolean");
  assertArray(data.inputs.staleness.reasons, "$.inputs.staleness.reasons");
  for (let i = 0; i < data.inputs.staleness.reasons.length; i += 1) assertNonUuidString(data.inputs.staleness.reasons[i], `$.inputs.staleness.reasons[${i}]`, { minLength: 1 });

  const cursor = assertNonNegativeInt(data.question_cursor, "$.question_cursor");
  const asked = assertNonNegativeInt(data.asked_count, "$.asked_count");
  const answered = assertNonNegativeInt(data.answered_count, "$.answered_count");
  if (answered > asked) fail("$.answered_count", "must be <= asked_count");
  if (cursor !== asked) fail("$.question_cursor", "must equal asked_count");

  assertIsoDateTimeZ(data.created_at, "$.created_at");
  assertIsoDateTimeZ(data.updated_at, "$.updated_at");
  if (data.closed_at !== null) assertIsoDateTimeZ(data.closed_at, "$.closed_at");
  if (data.closed_decision !== null) assertNonUuidString(data.closed_decision, "$.closed_decision", { minLength: 1 });

  if (data.status === "waiting_for_answer") {
    if (asked === answered) fail("$.status", "waiting_for_answer requires asked_count > answered_count");
  }
  if (data.status !== "waiting_for_answer") {
    if (asked > answered) fail("$.status", "must be waiting_for_answer when there is an unanswered question");
  }
  if (data.status === "closed") {
    if (data.closed_at === null) fail("$.closed_at", "required when status is closed");
    if (data.closed_decision === null) fail("$.closed_decision", "required when status is closed");
  }
  if (data.status !== "closed") {
    if (data.closed_at !== null) fail("$.closed_at", "must be null unless status is closed");
    if (data.closed_decision !== null) fail("$.closed_decision", "must be null unless status is closed");
  }

  return data;
}

