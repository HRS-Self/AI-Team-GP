import { assertArray, assertBoolean, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

function hasContextHint(s) {
  const lower = String(s || "").toLowerCase();
  return lower.includes("file:") || lower.includes("path:") || lower.includes("endpoint:");
}

function looksLikeIdishToken(s) {
  const str = String(s || "").trim();
  if (!str) return false;
  if (/^[a-f0-9]{16,}$/i.test(str)) return true;
  if (/^[A-Z][A-Z0-9_]{2,}:.+/.test(str)) return true; // e.g., SSOT:..., EVID:...
  if (/^(EVID|CLAIM|CHAL|GAP|DEC|Q|FACT)_[A-Za-z0-9]+$/.test(str)) return true;
  return false;
}

export function validateCommitteeStatus(data) {
  assertPlainObject(data, "$");
  const allowedTop = new Set([
    "version",
    "repo_id",
    "evidence_valid",
    "blocking_issues",
    "confidence",
    "next_action",
    "degraded",
    "degraded_reason",
    "stale",
    "hard_stale",
    "staleness",
  ]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  assertNonUuidString(data.repo_id, "$.repo_id", { minLength: 1 });
  assertBoolean(data.evidence_valid, "$.evidence_valid");
  assertArray(data.blocking_issues, "$.blocking_issues");

  for (let i = 0; i < data.blocking_issues.length; i += 1) {
    const it = data.blocking_issues[i];
    assertPlainObject(it, `$.blocking_issues[${i}]`);
    const allowed = new Set(["id", "description", "evidence_missing", "severity"]);
    for (const k of Object.keys(it)) if (!allowed.has(k)) fail(`$.blocking_issues[${i}].${k}`, "unknown field");
    assertNonUuidString(it.id, `$.blocking_issues[${i}].id`, { minLength: 8 });
    assertNonUuidString(it.description, `$.blocking_issues[${i}].description`, { minLength: 1 });
    assertArray(it.evidence_missing, `$.blocking_issues[${i}].evidence_missing`);
    for (let j = 0; j < it.evidence_missing.length; j += 1) {
      const s = assertNonUuidString(it.evidence_missing[j], `$.blocking_issues[${i}].evidence_missing[${j}]`, { minLength: 12 });
      if (looksLikeIdishToken(s) && !hasContextHint(s)) {
        fail(`$.blocking_issues[${i}].evidence_missing[${j}]`, "must be descriptive (include file:/path:/endpoint: context)");
      }
    }
    const sev = assertNonUuidString(it.severity, `$.blocking_issues[${i}].severity`, { minLength: 1 });
    if (!new Set(["high", "medium", "low"]).has(sev)) fail(`$.blocking_issues[${i}].severity`, "invalid");
  }

  const conf = assertNonUuidString(data.confidence, "$.confidence", { minLength: 1 });
  if (!new Set(["low", "medium", "high"]).has(conf)) fail("$.confidence", "invalid");
  const na = assertNonUuidString(data.next_action, "$.next_action", { minLength: 1 });
  if (!new Set(["proceed", "decision_needed", "rescan_needed"]).has(na)) fail("$.next_action", "invalid");

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
