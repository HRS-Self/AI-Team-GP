import { assertArray, assertBoolean, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

export function validateIntegrationStatus(data) {
  assertPlainObject(data, "$");
  const allowedTop = new Set([
    "version",
    "evidence_valid",
    "integration_gaps",
    "decision_needed",
    "degraded",
    "degraded_reason",
    "stale",
    "hard_stale",
    "staleness",
  ]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  assertBoolean(data.evidence_valid, "$.evidence_valid");
  assertBoolean(data.decision_needed, "$.decision_needed");
  assertArray(data.integration_gaps, "$.integration_gaps");

  for (let i = 0; i < data.integration_gaps.length; i += 1) {
    const g = data.integration_gaps[i];
    assertPlainObject(g, `$.integration_gaps[${i}]`);
    const allowed = new Set(["id", "repos", "description", "evidence_refs", "severity"]);
    for (const k of Object.keys(g)) if (!allowed.has(k)) fail(`$.integration_gaps[${i}].${k}`, "unknown field");
    assertNonUuidString(g.id, `$.integration_gaps[${i}].id`, { minLength: 8 });
    assertArray(g.repos, `$.integration_gaps[${i}].repos`, { minItems: 1 });
    for (let j = 0; j < g.repos.length; j += 1) assertNonUuidString(g.repos[j], `$.integration_gaps[${i}].repos[${j}]`, { minLength: 1 });
    assertNonUuidString(g.description, `$.integration_gaps[${i}].description`, { minLength: 1 });
    assertArray(g.evidence_refs, `$.integration_gaps[${i}].evidence_refs`);
    for (let j = 0; j < g.evidence_refs.length; j += 1) assertNonUuidString(g.evidence_refs[j], `$.integration_gaps[${i}].evidence_refs[${j}]`, { minLength: 1 });
    const sev = assertNonUuidString(g.severity, `$.integration_gaps[${i}].severity`, { minLength: 1 });
    if (!new Set(["high", "medium", "low"]).has(sev)) fail(`$.integration_gaps[${i}].severity`, "invalid");
  }

  if (Object.prototype.hasOwnProperty.call(data, "degraded")) assertBoolean(data.degraded, "$.degraded");
  if (Object.prototype.hasOwnProperty.call(data, "degraded_reason")) {
    const dr = assertNonUuidString(data.degraded_reason, "$.degraded_reason", { minLength: 1 });
    if (!new Set(["soft_stale"]).has(dr)) fail("$.degraded_reason", "invalid");
  }
  if (Object.prototype.hasOwnProperty.call(data, "stale")) assertBoolean(data.stale, "$.stale");
  if (Object.prototype.hasOwnProperty.call(data, "hard_stale")) assertBoolean(data.hard_stale, "$.hard_stale");
  if (Object.prototype.hasOwnProperty.call(data, "staleness")) assertPlainObject(data.staleness, "$.staleness");

  return data;
}
