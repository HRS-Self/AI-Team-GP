import { assertArray, assertFsSafeUtcTimestamp, assertInt, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

export function validateKnowledgeScan(data) {
  assertPlainObject(data, "$");
  const allowedTop = new Set(["repo_id", "scanned_at", "facts", "unknowns", "contradictions", "scan_version", "coverage", "external_knowledge"]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  assertNonUuidString(data.repo_id, "$.repo_id", { minLength: 1 });
  assertFsSafeUtcTimestamp(data.scanned_at, "$.scanned_at");
  assertInt(data.scan_version, "$.scan_version", { min: 1 });

  assertArray(data.external_knowledge, "$.external_knowledge");
  for (let i = 0; i < data.external_knowledge.length; i += 1) {
    const ek = data.external_knowledge[i];
    assertPlainObject(ek, `$.external_knowledge[${i}]`);
    const allowed = new Set(["project_code", "repo_id", "bundle_id", "path", "loaded_at"]);
    for (const k of Object.keys(ek)) if (!allowed.has(k)) fail(`$.external_knowledge[${i}].${k}`, "unknown field");
    assertNonUuidString(ek.project_code, `$.external_knowledge[${i}].project_code`, { minLength: 1 });
    assertNonUuidString(ek.repo_id, `$.external_knowledge[${i}].repo_id`, { minLength: 1 });
    assertNonUuidString(ek.bundle_id, `$.external_knowledge[${i}].bundle_id`, { minLength: 1 });
    assertNonUuidString(ek.path, `$.external_knowledge[${i}].path`, { minLength: 1 });
    // loaded_at must be canonical ISO
    if (typeof ek.loaded_at !== "string") fail(`$.external_knowledge[${i}].loaded_at`, "must be a string");
    const ms = Date.parse(ek.loaded_at);
    if (!Number.isFinite(ms)) fail(`$.external_knowledge[${i}].loaded_at`, "must be a valid ISO timestamp");
    if (new Date(ms).toISOString() !== ek.loaded_at) fail(`$.external_knowledge[${i}].loaded_at`, "must be a canonical ISO timestamp (Date.toISOString())");
  }

  assertArray(data.facts, "$.facts");
  const factIds = new Set();
  for (let i = 0; i < data.facts.length; i += 1) {
    const f = data.facts[i];
    assertPlainObject(f, `$.facts[${i}]`);
    const allowed = new Set(["fact_id", "claim", "evidence_ids"]);
    for (const k of Object.keys(f)) if (!allowed.has(k)) fail(`$.facts[${i}].${k}`, "unknown field");
    const factId = assertNonUuidString(f.fact_id, `$.facts[${i}].fact_id`, { minLength: 1 });
    if (factIds.has(factId)) fail(`$.facts[${i}].fact_id`, "duplicate fact_id");
    factIds.add(factId);
    assertNonUuidString(f.claim, `$.facts[${i}].claim`, { minLength: 1 });
    assertArray(f.evidence_ids, `$.facts[${i}].evidence_ids`, { minItems: 1 });
    const ids = new Set();
    for (let j = 0; j < f.evidence_ids.length; j += 1) {
      const id = assertNonUuidString(f.evidence_ids[j], `$.facts[${i}].evidence_ids[${j}]`, { minLength: 1 });
      if (ids.has(id)) fail(`$.facts[${i}].evidence_ids[${j}]`, "duplicate evidence_id");
      ids.add(id);
    }
  }

  assertArray(data.unknowns, "$.unknowns");
  for (let i = 0; i < data.unknowns.length; i += 1) assertNonUuidString(data.unknowns[i], `$.unknowns[${i}]`, { minLength: 1 });

  assertArray(data.contradictions, "$.contradictions");
  for (let i = 0; i < data.contradictions.length; i += 1) {
    const c = data.contradictions[i];
    assertPlainObject(c, `$.contradictions[${i}]`);
    const allowed = new Set(["a_fact_id", "b_fact_id", "reason"]);
    for (const k of Object.keys(c)) if (!allowed.has(k)) fail(`$.contradictions[${i}].${k}`, "unknown field");
    const a = assertNonUuidString(c.a_fact_id, `$.contradictions[${i}].a_fact_id`, { minLength: 1 });
    const b = assertNonUuidString(c.b_fact_id, `$.contradictions[${i}].b_fact_id`, { minLength: 1 });
    if (!factIds.has(a)) fail(`$.contradictions[${i}].a_fact_id`, "must reference an existing fact_id");
    if (!factIds.has(b)) fail(`$.contradictions[${i}].b_fact_id`, "must reference an existing fact_id");
    assertNonUuidString(c.reason, `$.contradictions[${i}].reason`, { minLength: 1 });
  }

  assertPlainObject(data.coverage, "$.coverage");
  const allowedCov = new Set(["files_seen", "files_indexed"]);
  for (const k of Object.keys(data.coverage)) if (!allowedCov.has(k)) fail(`$.coverage.${k}`, "unknown field");
  assertInt(data.coverage.files_seen, "$.coverage.files_seen", { min: 0 });
  assertInt(data.coverage.files_indexed, "$.coverage.files_indexed", { min: 0 });

  return data;
}
