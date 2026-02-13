import { assertArray, assertBoolean, assertEnumString, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

function assertScope(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 1 });
  if (s !== "system" && !/^repo:[A-Za-z0-9._-]+$/.test(s)) fail(path, "must be 'system' or 'repo:<repo_id>'");
  return s;
}

function assertEvidenceMissingEntry(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 12 });
  const hasContext = /(file:|path:|endpoint:)/i.test(s);
  if (!hasContext) {
    if (/^[a-f0-9]{16,}$/i.test(s)) fail(path, "must be descriptive (looks like an ID/hash)");
    if (/^(SSOT|EVID|CLAIM|CHAL|GAP|DEC|Q|FACT)_[A-Za-z0-9]+$/.test(s)) fail(path, "must be descriptive (looks like an ID token)");
    if (/^[A-Z][A-Z0-9_]{2,}:.+/.test(s)) fail(path, "must be descriptive (looks like an opaque ref)");
  }
  return s;
}

function assertUniqueStrings(arr, path, { minLength = 1 } = {}) {
  const seen = new Set();
  for (let i = 0; i < arr.length; i += 1) {
    const s = assertNonUuidString(arr[i], `${path}[${i}]`, { minLength });
    if (seen.has(s)) fail(`${path}[${i}]`, "duplicate value");
    seen.add(s);
  }
  return arr;
}

function validateObligation(obj, path) {
  assertPlainObject(obj, path);
  const allowed = new Set(["required", "why", "suggested_test_directives", "target_paths"]);
  for (const k of Object.keys(obj)) if (!allowed.has(k)) fail(`${path}.${k}`, "unknown field");
  assertBoolean(obj.required, `${path}.required`);
  if (typeof obj.why !== "string") fail(`${path}.why`, "must be a string");
  assertArray(obj.suggested_test_directives, `${path}.suggested_test_directives`);
  for (let i = 0; i < obj.suggested_test_directives.length; i += 1) assertNonUuidString(obj.suggested_test_directives[i], `${path}.suggested_test_directives[${i}]`, { minLength: 1 });
  assertArray(obj.target_paths, `${path}.target_paths`);
  for (let i = 0; i < obj.target_paths.length; i += 1) assertNonUuidString(obj.target_paths[i], `${path}.target_paths[${i}]`, { minLength: 1 });
}

export function validateQaCommitteeOutput(data) {
  assertPlainObject(data, "$");

  const allowedTop = new Set(["version", "role", "scope", "created_at", "risk", "required_invariants", "test_obligations", "facts", "unknowns"]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  if (data.role !== "qa_strategist") fail("$.role", "must be qa_strategist");
  assertScope(data.scope, "$.scope");
  assertIsoDateTimeZ(data.created_at, "$.created_at");

  assertPlainObject(data.risk, "$.risk");
  {
    const r = data.risk;
    const allowed = new Set(["level", "notes"]);
    for (const k of Object.keys(r)) if (!allowed.has(k)) fail(`$.risk.${k}`, "unknown field");
    assertEnumString(r.level, "$.risk.level", ["low", "normal", "high", "unknown"]);
    if (typeof r.notes !== "string") fail("$.risk.notes", "must be a string");
  }

  assertArray(data.required_invariants, "$.required_invariants");
  for (let i = 0; i < data.required_invariants.length; i += 1) {
    const inv = data.required_invariants[i];
    assertPlainObject(inv, `$.required_invariants[${i}]`);
    const allowed = new Set(["id", "text", "severity", "evidence_refs", "evidence_missing"]);
    for (const k of Object.keys(inv)) if (!allowed.has(k)) fail(`$.required_invariants[${i}].${k}`, "unknown field");
    assertNonUuidString(inv.id, `$.required_invariants[${i}].id`, { minLength: 1 });
    assertNonUuidString(inv.text, `$.required_invariants[${i}].text`, { minLength: 1 });
    assertEnumString(inv.severity, `$.required_invariants[${i}].severity`, ["high", "medium", "low"]);
    assertArray(inv.evidence_refs, `$.required_invariants[${i}].evidence_refs`);
    assertUniqueStrings(inv.evidence_refs, `$.required_invariants[${i}].evidence_refs`, { minLength: 1 });
    assertArray(inv.evidence_missing, `$.required_invariants[${i}].evidence_missing`);
    for (let j = 0; j < inv.evidence_missing.length; j += 1) assertEvidenceMissingEntry(inv.evidence_missing[j], `$.required_invariants[${i}].evidence_missing[${j}]`);
    if (inv.evidence_refs.length === 0 && inv.evidence_missing.length === 0) fail(`$.required_invariants[${i}]`, "must include evidence_refs or evidence_missing");
  }

  assertPlainObject(data.test_obligations, "$.test_obligations");
  {
    const o = data.test_obligations;
    const allowed = new Set(["unit", "integration", "e2e"]);
    for (const k of Object.keys(o)) if (!allowed.has(k)) fail(`$.test_obligations.${k}`, "unknown field");
    validateObligation(o.unit, "$.test_obligations.unit");
    validateObligation(o.integration, "$.test_obligations.integration");
    validateObligation(o.e2e, "$.test_obligations.e2e");
  }

  assertArray(data.facts, "$.facts");
  for (let i = 0; i < data.facts.length; i += 1) {
    const it = data.facts[i];
    assertPlainObject(it, `$.facts[${i}]`);
    const allowed = new Set(["text", "evidence_refs"]);
    for (const k of Object.keys(it)) if (!allowed.has(k)) fail(`$.facts[${i}].${k}`, "unknown field");
    assertNonUuidString(it.text, `$.facts[${i}].text`, { minLength: 1 });
    assertArray(it.evidence_refs, `$.facts[${i}].evidence_refs`, { minItems: 1 });
    assertUniqueStrings(it.evidence_refs, `$.facts[${i}].evidence_refs`, { minLength: 1 });
  }

  assertArray(data.unknowns, "$.unknowns");
  for (let i = 0; i < data.unknowns.length; i += 1) {
    const it = data.unknowns[i];
    assertPlainObject(it, `$.unknowns[${i}]`);
    const allowed = new Set(["text", "evidence_missing"]);
    for (const k of Object.keys(it)) if (!allowed.has(k)) fail(`$.unknowns[${i}].${k}`, "unknown field");
    assertNonUuidString(it.text, `$.unknowns[${i}].text`, { minLength: 1 });
    assertArray(it.evidence_missing, `$.unknowns[${i}].evidence_missing`, { minItems: 1 });
    for (let j = 0; j < it.evidence_missing.length; j += 1) assertEvidenceMissingEntry(it.evidence_missing[j], `$.unknowns[${i}].evidence_missing[${j}]`);
  }

  return data;
}

