import { fail } from "./error.js";
import { assertArray, assertHex64, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { sha256Hex } from "../../utils/fs-hash.js";

const MAX_SKILL_MD_BYTES = 80 * 1024;
const SKILL_ID_RE = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
const APPLY_SCOPE_RE = /^(system|repo:[A-Za-z0-9._-]+|project:[A-Za-z0-9._-]+)$/;
const SOURCE_SCOPE_RE = /^(system|repo:[A-Za-z0-9._-]+)$/;

function normalizeLf(text) {
  return String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

function assertNoForbiddenText(value, path) {
  const s = String(value || "");
  if (/\/opt\//i.test(s)) fail(path, "must not contain '/opt/'");
  if (/(^|[\s"'`(])\/[A-Za-z0-9_.-][^\s"'`)]*/.test(s)) fail(path, "must not contain absolute filesystem paths");
  if (/\b(api[_-]?key|secret|password|auth[_-]?token)\b/i.test(s)) fail(path, "must not include secrets/tokens");
  if (/\bsk-[A-Za-z0-9]{16,}\b/.test(s)) fail(path, "must not include secret tokens");
}

function assertSkillId(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 3 });
  if (!SKILL_ID_RE.test(s)) fail(path, "must be kebab-case");
  return s;
}

function assertApplyScope(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 1 });
  if (!APPLY_SCOPE_RE.test(s)) fail(path, "must be one of: system | repo:<id> | project:<code>");
  return s;
}

function assertSourceScope(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 1 });
  if (!SOURCE_SCOPE_RE.test(s)) fail(path, "must be one of: system | repo:<id>");
  return s;
}

export function validateSkillPackage(metadata, { skillMd = null } = {}) {
  assertPlainObject(metadata, "$");

  const allowedTop = new Set([
    "version",
    "skill_id",
    "title",
    "domain",
    "applies_to",
    "created_at",
    "updated_at",
    "hash",
    "evidence_refs",
    "source_scope",
    "dependencies",
    "author",
  ]);
  for (const key of Object.keys(metadata)) if (!allowedTop.has(key)) fail(`$.${key}`, "unknown field");

  if (metadata.version !== 1) fail("$.version", "must be 1");
  assertSkillId(metadata.skill_id, "$.skill_id");
  assertNonUuidString(metadata.title, "$.title", { minLength: 1 });
  assertNonUuidString(metadata.domain, "$.domain", { minLength: 1 });
  assertNoForbiddenText(metadata.title, "$.title");
  assertNoForbiddenText(metadata.domain, "$.domain");

  assertArray(metadata.applies_to, "$.applies_to", { minItems: 1 });
  for (let i = 0; i < metadata.applies_to.length; i += 1) {
    const scope = assertApplyScope(metadata.applies_to[i], `$.applies_to[${i}]`);
    assertNoForbiddenText(scope, `$.applies_to[${i}]`);
  }

  assertIsoDateTimeZ(metadata.created_at, "$.created_at");
  assertIsoDateTimeZ(metadata.updated_at, "$.updated_at");
  assertHex64(metadata.hash, "$.hash");

  assertArray(metadata.evidence_refs, "$.evidence_refs", { minItems: 1 });
  for (let i = 0; i < metadata.evidence_refs.length; i += 1) {
    const ref = assertNonUuidString(metadata.evidence_refs[i], `$.evidence_refs[${i}]`, { minLength: 1 });
    assertNoForbiddenText(ref, `$.evidence_refs[${i}]`);
  }

  assertSourceScope(metadata.source_scope, "$.source_scope");
  assertArray(metadata.dependencies, "$.dependencies");
  for (let i = 0; i < metadata.dependencies.length; i += 1) {
    const dep = assertSkillId(metadata.dependencies[i], `$.dependencies[${i}]`);
    if (dep === metadata.skill_id) fail(`$.dependencies[${i}]`, "must not self-reference skill_id");
  }

  if (metadata.author !== "skill.author") fail("$.author", "must be 'skill.author'");

  if (typeof skillMd === "string") {
    const normalized = normalizeLf(skillMd);
    assertNoForbiddenText(normalized, "$skill_md");
    const bytes = Buffer.byteLength(normalized, "utf8");
    if (bytes > MAX_SKILL_MD_BYTES) fail("$skill_md", `must be <= ${MAX_SKILL_MD_BYTES} bytes`);
    const actualHash = sha256Hex(normalized);
    if (actualHash !== metadata.hash) fail("$.hash", `hash mismatch (expected ${metadata.hash}, got ${actualHash})`);
  }

  return metadata;
}

