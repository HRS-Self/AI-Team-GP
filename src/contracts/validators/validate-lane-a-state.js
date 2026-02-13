import { assertArray, assertBoolean, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

export function validateLaneAState(data) {
  assertPlainObject(data, "$");
  const allowedTop = new Set(["version", "stage", "evidence_state", "next_action"]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");

  const stage = assertNonUuidString(data.stage, "$.stage", { minLength: 1 });
  const stages = new Set([
    "NEEDS_INDEX",
    "NEEDS_SCAN",
    "NEEDS_KICKOFF",
    "REFRESH_NEEDED",
    "DECISION_NEEDED",
    "DECISION_ANSWERED",
    "COMMITTEE_PENDING",
    "COMMITTEE_REPO_FAILED",
    "COMMITTEE_REPO_PASSED",
    "COMMITTEE_INTEGRATION_FAILED",
    "COMMITTEE_PASSED",
    "READY_FOR_WRITER",
  ]);
  if (!stages.has(stage)) fail("$.stage", "invalid");

  assertPlainObject(data.evidence_state, "$.evidence_state");
  const esAllowed = new Set([
    "evidence_level",
    "scan_coverage_complete",
    "minimum_sufficient",
    "milestone_status",
    "last_scan_at",
    "last_index_at",
    "last_synth_at",
    "pending_events",
  ]);
  for (const k of Object.keys(data.evidence_state)) if (!esAllowed.has(k)) fail(`$.evidence_state.${k}`, "unknown field");

  const level = assertNonUuidString(data.evidence_state.evidence_level, "$.evidence_state.evidence_level", { minLength: 1 });
  if (!new Set(["none", "partial", "complete"]).has(level)) fail("$.evidence_state.evidence_level", "must be one of: none, partial, complete");

  assertBoolean(data.evidence_state.scan_coverage_complete, "$.evidence_state.scan_coverage_complete");
  assertBoolean(data.evidence_state.minimum_sufficient, "$.evidence_state.minimum_sufficient");
  assertPlainObject(data.evidence_state.milestone_status, "$.evidence_state.milestone_status");

  const nullableTs = (v, path) => {
    if (v === null) return;
    assertNonUuidString(v, path, { minLength: 1 });
  };
  nullableTs(data.evidence_state.last_scan_at, "$.evidence_state.last_scan_at");
  nullableTs(data.evidence_state.last_index_at, "$.evidence_state.last_index_at");
  nullableTs(data.evidence_state.last_synth_at, "$.evidence_state.last_synth_at");
  if (!Number.isFinite(Number(data.evidence_state.pending_events)) || Number(data.evidence_state.pending_events) < 0) fail("$.evidence_state.pending_events", "must be >= 0");

  assertPlainObject(data.next_action, "$.next_action");
  const naAllowed = new Set(["type", "target_repos", "reason"]);
  for (const k of Object.keys(data.next_action)) if (!naAllowed.has(k)) fail(`$.next_action.${k}`, "unknown field");
  const type = assertNonUuidString(data.next_action.type, "$.next_action.type", { minLength: 1 });
  if (!new Set(["index", "scan", "refresh", "kickoff", "committee", "synthesize", "question", "ready"]).has(type)) fail("$.next_action.type", "invalid");
  assertArray(data.next_action.target_repos, "$.next_action.target_repos");
  for (let i = 0; i < data.next_action.target_repos.length; i += 1) assertNonUuidString(data.next_action.target_repos[i], `$.next_action.target_repos[${i}]`, { minLength: 1 });
  assertNonUuidString(data.next_action.reason, "$.next_action.reason", { minLength: 1 });

  return data;
}
