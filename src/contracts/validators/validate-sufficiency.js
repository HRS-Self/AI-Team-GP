import { assertEnumString, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function assertScope(s, path) {
  const v = assertNonUuidString(s, path, { minLength: 1 });
  if (v !== "system" && !/^repo:[A-Za-z0-9._-]+$/.test(v)) fail(path, "must be 'system' or 'repo:<repo_id>'");
  return v;
}

function assertKnowledgeVersion(v, path) {
  const s = assertNonUuidString(v, path, { minLength: 2 });
  if (!/^v\d+(\.\d+)*$/.test(s)) fail(path, "must match v<major>[.<minor>[.<patch>...]]");
  return s;
}

function assertStringArray(arr, path, { minLen = 0, maxItems = 200, itemMinLen = 1 } = {}) {
  if (!Array.isArray(arr)) fail(path, "must be an array");
  if (arr.length < minLen) fail(path, `must have at least ${minLen} items`);
  if (arr.length > maxItems) fail(path, `must have at most ${maxItems} items`);
  const out = [];
  for (let i = 0; i < arr.length; i += 1) {
    const s = assertNonUuidString(arr[i], `${path}[${i}]`, { minLength: itemMinLen });
    out.push(normStr(s));
  }
  return out;
}

function assertBlockers(arr, path) {
  if (!Array.isArray(arr)) fail(path, "must be an array");
  if (arr.length > 200) fail(path, "must have at most 200 items");
  const out = [];
  for (let i = 0; i < arr.length; i += 1) {
    const b = arr[i];
    assertPlainObject(b, `${path}[${i}]`);
    const allowed = new Set(["id", "title", "details"]);
    for (const k of Object.keys(b)) if (!allowed.has(k)) fail(`${path}[${i}].${k}`, "unknown field");
    const id = assertNonUuidString(b.id, `${path}[${i}].id`, { minLength: 1 });
    const title = assertNonUuidString(b.title, `${path}[${i}].title`, { minLength: 1 });
    const details = assertNonUuidString(b.details, `${path}[${i}].details`, { minLength: 1 });
    out.push({ id: normStr(id), title: normStr(title), details: normStr(details) });
  }
  return out;
}

export function validateSufficiency(data) {
  assertPlainObject(data, "$");
  const allowed = new Set([
    "version",
    "scope",
    "knowledge_version",
    "status",
    "decided_by",
    "decided_at",
    "rationale_md_path",
    "evidence_basis",
    "blockers",
    "stale_status",
  ]);
  for (const k of Object.keys(data)) if (!allowed.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  const scope = assertScope(data.scope, "$.scope");
  const knowledge_version = assertKnowledgeVersion(data.knowledge_version, "$.knowledge_version");
  const status = assertEnumString(data.status, "$.status", ["insufficient", "proposed_sufficient", "sufficient"]);
  const stale_status = assertEnumString(data.stale_status, "$.stale_status", ["fresh", "soft_stale", "hard_stale"]);

  const evidence_basis = assertStringArray(data.evidence_basis, "$.evidence_basis", { minLen: 0, maxItems: 200, itemMinLen: 3 });
  const blockers = assertBlockers(data.blockers, "$.blockers");

  const decided_by = data.decided_by === null || data.decided_by === undefined ? null : assertNonUuidString(data.decided_by, "$.decided_by", { minLength: 1 });
  const decided_at = data.decided_at === null || data.decided_at === undefined ? null : assertIsoDateTimeZ(data.decided_at, "$.decided_at");

  if (data.rationale_md_path !== null && data.rationale_md_path !== undefined) {
    const p = assertNonUuidString(data.rationale_md_path, "$.rationale_md_path", { minLength: 1 });
    if (!p.startsWith("/")) fail("$.rationale_md_path", "must be an absolute path");
  }

  if (status === "sufficient") {
    if (decided_by === null) fail("$.decided_by", "required when status is sufficient");
    if (decided_at === null) fail("$.decided_at", "required when status is sufficient");
  } else {
    if (decided_by !== null) fail("$.decided_by", "must be null unless status is sufficient");
    if (decided_at !== null) fail("$.decided_at", "must be null unless status is sufficient");
  }

  return {
    ...data,
    scope,
    knowledge_version,
    status,
    stale_status,
    evidence_basis,
    blockers,
    decided_by: decided_by === null ? null : normStr(decided_by),
    decided_at,
  };
}
