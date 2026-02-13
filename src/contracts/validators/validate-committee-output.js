import { assertArray, assertEnumString, assertNonUuidString, assertNumber, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

function assertScope(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 1 });
  if (s !== "system" && !/^repo:[A-Za-z0-9._-]+$/.test(s)) fail(path, "must be 'system' or 'repo:<repo_id>'");
  return s;
}

function assertEvidenceMissingEntry(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 12 });
  // Reject opaque ID-ish strings unless they also contain actionable context.
  const hasContext = /(file:|path:|endpoint:)/i.test(s);
  if (!hasContext) {
    if (/^[a-f0-9]{16,}$/i.test(s)) fail(path, "must be descriptive (looks like an ID/hash)");
    if (/^(SSOT|EVID|CLAIM|CHAL|GAP|DEC|Q|FACT)_[A-Za-z0-9]+$/.test(s)) fail(path, "must be descriptive (looks like an ID token)");
    if (/^[A-Z][A-Z0-9_]{2,}:.+/.test(s)) fail(path, "must be descriptive (looks like an opaque ref)");
  }
  return s;
}

function assertUniqueStrings(arr, path) {
  const seen = new Set();
  for (let i = 0; i < arr.length; i += 1) {
    const s = assertNonUuidString(arr[i], `${path}[${i}]`, { minLength: 1 });
    if (seen.has(s)) fail(`${path}[${i}]`, "duplicate value");
    seen.add(s);
  }
  return arr;
}

export function validateCommitteeOutput(data) {
  assertPlainObject(data, "$");
  const allowedTop = new Set(["scope", "stale", "facts", "assumptions", "unknowns", "integration_edges", "risks", "verdict"]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  assertScope(data.scope, "$.scope");
  if (Object.prototype.hasOwnProperty.call(data, "stale")) {
    if (typeof data.stale !== "boolean") fail("$.stale", "must be boolean");
  }

  assertArray(data.facts, "$.facts");
  for (let i = 0; i < data.facts.length; i += 1) {
    const it = data.facts[i];
    assertPlainObject(it, `$.facts[${i}]`);
    const allowed = new Set(["text", "evidence_refs"]);
    for (const k of Object.keys(it)) if (!allowed.has(k)) fail(`$.facts[${i}].${k}`, "unknown field");
    assertNonUuidString(it.text, `$.facts[${i}].text`, { minLength: 1 });
    assertArray(it.evidence_refs, `$.facts[${i}].evidence_refs`, { minItems: 1 });
    assertUniqueStrings(it.evidence_refs, `$.facts[${i}].evidence_refs`);
  }

  assertArray(data.assumptions, "$.assumptions");
  for (let i = 0; i < data.assumptions.length; i += 1) {
    const it = data.assumptions[i];
    assertPlainObject(it, `$.assumptions[${i}]`);
    const allowed = new Set(["text", "evidence_missing"]);
    for (const k of Object.keys(it)) if (!allowed.has(k)) fail(`$.assumptions[${i}].${k}`, "unknown field");
    assertNonUuidString(it.text, `$.assumptions[${i}].text`, { minLength: 1 });
    assertArray(it.evidence_missing, `$.assumptions[${i}].evidence_missing`, { minItems: 1 });
    for (let j = 0; j < it.evidence_missing.length; j += 1) assertEvidenceMissingEntry(it.evidence_missing[j], `$.assumptions[${i}].evidence_missing[${j}]`);
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

  assertArray(data.integration_edges, "$.integration_edges");
  for (let i = 0; i < data.integration_edges.length; i += 1) {
    const it = data.integration_edges[i];
    assertPlainObject(it, `$.integration_edges[${i}]`);
    const allowed = new Set(["from", "to", "type", "contract", "confidence", "evidence_refs", "evidence_missing"]);
    for (const k of Object.keys(it)) if (!allowed.has(k)) fail(`$.integration_edges[${i}].${k}`, "unknown field");
    assertNonUuidString(it.from, `$.integration_edges[${i}].from`, { minLength: 1 });
    assertNonUuidString(it.to, `$.integration_edges[${i}].to`, { minLength: 1 });
    assertEnumString(it.type, `$.integration_edges[${i}].type`, ["http", "event", "db", "sharedlib"]);
    assertNonUuidString(it.contract, `$.integration_edges[${i}].contract`, { minLength: 1 });
    assertNumber(it.confidence, `$.integration_edges[${i}].confidence`);
    if (it.confidence < 0 || it.confidence > 1) fail(`$.integration_edges[${i}].confidence`, "must be between 0 and 1");
    assertArray(it.evidence_refs, `$.integration_edges[${i}].evidence_refs`);
    assertUniqueStrings(it.evidence_refs, `$.integration_edges[${i}].evidence_refs`);
    assertArray(it.evidence_missing, `$.integration_edges[${i}].evidence_missing`);
    for (let j = 0; j < it.evidence_missing.length; j += 1) assertEvidenceMissingEntry(it.evidence_missing[j], `$.integration_edges[${i}].evidence_missing[${j}]`);
    if (it.evidence_refs.length === 0 && it.evidence_missing.length === 0) fail(`$.integration_edges[${i}]`, "must include evidence_refs or evidence_missing");
  }

  assertArray(data.risks, "$.risks");
  for (let i = 0; i < data.risks.length; i += 1) assertNonUuidString(data.risks[i], `$.risks[${i}]`, { minLength: 1 });

  assertEnumString(data.verdict, "$.verdict", ["evidence_valid", "evidence_invalid"]);
  return data;
}
