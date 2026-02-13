import { createHash } from "node:crypto";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function normalizeBranchName(x) {
  if (!isNonEmptyString(x)) return null;
  const s = x.trim();
  if (s.startsWith("/")) return null;
  if (s.includes("..")) return null;
  return s;
}

function isVersionLike(v) {
  return /^v\d+(\.\d+)*$/.test(String(v || "").trim());
}

export function validateTriagedRepoItem(raw, { triagedId = null, rawIntakeId = null, createdAt = null } = {}) {
  const errors = [];
  const add = (m) => errors.push(m);
  if (!isPlainObject(raw)) return { ok: false, errors: ["Triaged repo item must be a JSON object."], normalized: null };
  if (raw.version !== 1) add("version must be 1.");

  const triaged_id = triagedId || (isNonEmptyString(raw.triaged_id) ? raw.triaged_id.trim() : null);
  const raw_intake_id = rawIntakeId || (isNonEmptyString(raw.raw_intake_id) ? raw.raw_intake_id.trim() : null);
  const created_at = createdAt || (isNonEmptyString(raw.created_at) ? raw.created_at.trim() : null);

  if (!isNonEmptyString(triaged_id)) add("triaged_id must be a non-empty string.");
  if (!isNonEmptyString(raw_intake_id)) add("raw_intake_id must be a non-empty string.");
  if (!isNonEmptyString(created_at)) add("created_at must be a non-empty string.");

  const repo_id = isNonEmptyString(raw.repo_id) ? raw.repo_id.trim() : null;
  const team_id = isNonEmptyString(raw.team_id) ? raw.team_id.trim() : null;
  const target_branch = normalizeBranchName(raw.target_branch);
  const summary = isNonEmptyString(raw.summary) ? raw.summary.trim() : null;
  const instructions = isNonEmptyString(raw.instructions) ? raw.instructions.trim() : null;
  const dedupe_key = isNonEmptyString(raw.dedupe_key) ? raw.dedupe_key.trim() : null;

  if (!repo_id) add("repo_id must be a non-empty string.");
  if (!team_id) add("team_id must be a non-empty string.");
  if (!target_branch) add("target_branch must be a non-empty branch name (not a path).");
  if (!summary) add("summary must be a non-empty string.");
  if (!instructions) add("instructions must be a non-empty string.");
  if (!dedupe_key) add("dedupe_key must be a non-empty string.");

  const normalized = {
    version: 1,
    triaged_id,
    raw_intake_id,
    created_at,
    repo_id: repo_id || "(missing)",
    team_id: team_id || "(missing)",
    target_branch: target_branch || "develop",
    summary: summary || "(missing)",
    instructions: instructions || "(missing)",
    dedupe_key: dedupe_key || sha256Hex(`${repo_id || ""}\n${summary || ""}`).slice(0, 16),
  };

  // Optional Lane A governance metadata (preserved verbatim for downstream enforcement).
  const origin = isNonEmptyString(raw.origin) ? raw.origin.trim() : null;
  if (origin && origin.toLowerCase() === "lane_a") {
    normalized.origin = "lane_a";
    const ia = isNonEmptyString(raw.intake_approval_id) ? raw.intake_approval_id.trim() : null;
    const kv = isNonEmptyString(raw.knowledge_version) && isVersionLike(raw.knowledge_version) ? raw.knowledge_version.trim() : null;
    const sc = isNonEmptyString(raw.lane_a_scope) ? raw.lane_a_scope.trim() : null;
    if (ia) normalized.intake_approval_id = ia;
    if (kv) normalized.knowledge_version = kv;
    if (sc) normalized.lane_a_scope = sc;
    normalized.sufficiency_override = raw.sufficiency_override === true;
  }

  return { ok: errors.length === 0, errors, normalized };
}

export function validateTriagedBatch(raw, { batchId = null, rawIntakeId = null, createdAt = null } = {}) {
  const errors = [];
  const add = (m) => errors.push(m);
  if (!isPlainObject(raw)) return { ok: false, errors: ["Batch must be a JSON object."], normalized: null };
  if (raw.version !== 1) add("version must be 1.");
  const batch_id = batchId || (isNonEmptyString(raw.batch_id) ? raw.batch_id.trim() : null);
  const raw_intake_id = rawIntakeId || (isNonEmptyString(raw.raw_intake_id) ? raw.raw_intake_id.trim() : null);
  const created_at = createdAt || (isNonEmptyString(raw.created_at) ? raw.created_at.trim() : null);
  if (!isNonEmptyString(batch_id)) add("batch_id must be a non-empty string.");
  if (!isNonEmptyString(raw_intake_id)) add("raw_intake_id must be a non-empty string.");
  if (!isNonEmptyString(created_at)) add("created_at must be a non-empty string.");
  const triaged_ids = Array.isArray(raw.triaged_ids) ? raw.triaged_ids.map((x) => String(x).trim()).filter(Boolean) : [];
  const repo_ids = Array.isArray(raw.repo_ids) ? raw.repo_ids.map((x) => String(x).trim()).filter(Boolean) : [];
  const status = isNonEmptyString(raw.status) ? raw.status.trim() : null;
  if (!triaged_ids.length) add("triaged_ids must be a non-empty string array.");
  if (!repo_ids.length) add("repo_ids must be a non-empty string array.");
  if (!(status === "triaged" || status === "swept")) add("status must be triaged|swept.");
  const normalized = { version: 1, batch_id, raw_intake_id, created_at, triaged_ids, repo_ids, status: status || "triaged" };
  return { ok: errors.length === 0, errors, normalized };
}
