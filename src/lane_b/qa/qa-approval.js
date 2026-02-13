import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

function nowISO() {
  return new Date().toISOString();
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function validateQaApprovalShape(doc) {
  if (!isPlainObject(doc)) throw new Error("QA_APPROVAL must be a JSON object.");
  const allowed = new Set(["version", "workId", "status", "by", "notes", "updated_at"]);
  for (const k of Object.keys(doc)) if (!allowed.has(k)) throw new Error(`QA_APPROVAL has unknown field: ${k}`);
  if (doc.version !== 1) throw new Error("QA_APPROVAL.version must be 1.");
  if (!normStr(doc.workId)) throw new Error("QA_APPROVAL.workId must be a non-empty string.");
  const st = normStr(doc.status).toLowerCase();
  if (!["pending", "approved", "rejected"].includes(st)) throw new Error("QA_APPROVAL.status must be pending|approved|rejected.");
  if (!(doc.by === null || typeof doc.by === "string")) throw new Error("QA_APPROVAL.by must be string|null.");
  if (!(doc.notes === null || typeof doc.notes === "string")) throw new Error("QA_APPROVAL.notes must be string|null.");
  if (!normStr(doc.updated_at)) throw new Error("QA_APPROVAL.updated_at must be an ISO timestamp string.");
  const ms = Date.parse(String(doc.updated_at));
  if (!Number.isFinite(ms)) throw new Error("QA_APPROVAL.updated_at must be a valid ISO timestamp.");
  const rt = new Date(ms).toISOString();
  if (rt !== doc.updated_at) throw new Error("QA_APPROVAL.updated_at must be canonical Date.toISOString().");
  return { ...doc, status: st };
}

function defaultApproval(workId) {
  return {
    version: 1,
    workId: String(workId),
    status: "pending",
    by: null,
    notes: null,
    updated_at: nowISO(),
  };
}

async function readTextAbsIfExists(absPath) {
  try {
    return await readFile(absPath, "utf8");
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return null;
    throw err;
  }
}

export function qaApprovalPaths({ projectRoot, workId }) {
  const projectRootAbs = resolve(String(projectRoot || ""));
  const wid = normStr(workId);
  if (!wid) throw new Error("Missing workId.");
  const workDirRel = `ai/lane_b/work/${wid}`;
  return {
    workId: wid,
    workDirRel,
    approvalRel: `${workDirRel}/QA_APPROVAL.json`,
    approvalAbs: join(projectRootAbs, workDirRel, "QA_APPROVAL.json"),
  };
}

export async function readQaApprovalOrDefault({ projectRoot, workId }) {
  const p = qaApprovalPaths({ projectRoot, workId });
  const text = await readTextAbsIfExists(p.approvalAbs);
  if (!text) return { ok: true, exists: false, approval: defaultApproval(p.workId), path: p.approvalRel };
  let j;
  try {
    j = JSON.parse(text);
  } catch {
    return { ok: false, message: `Invalid JSON in ${p.approvalRel}.` };
  }
  try {
    const v = validateQaApprovalShape(j);
    return { ok: true, exists: true, approval: v, path: p.approvalRel };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Invalid QA_APPROVAL shape in ${p.approvalRel} (${msg}).` };
  }
}

export async function setQaApprovalStatus({ projectRoot, workId, status, by, notes = null, dryRun = false } = {}) {
  const wid = normStr(workId);
  if (!wid) return { ok: false, message: "Missing workId." };
  const st = normStr(status).toLowerCase();
  if (!["approved", "rejected"].includes(st)) return { ok: false, message: "Invalid status (expected approved|rejected)." };
  const who = normStr(by);
  if (!who) return { ok: false, message: "Missing by." };

  const projectRootAbs = resolve(String(projectRoot || ""));
  const p = qaApprovalPaths({ projectRoot: projectRootAbs, workId: wid });
  const prev = await readQaApprovalOrDefault({ projectRoot: projectRootAbs, workId: wid });
  if (!prev.ok) return prev;

  const next = {
    version: 1,
    workId: wid,
    status: st,
    by: who,
    notes: notes && String(notes).trim() ? String(notes).trim() : null,
    updated_at: nowISO(),
  };
  try {
    validateQaApprovalShape(next);
  } catch (err) {
    return { ok: false, message: err instanceof Error ? err.message : String(err) };
  }

  if (!dryRun) {
    await mkdir(join(projectRootAbs, p.workDirRel), { recursive: true });
    await writeFile(p.approvalAbs, JSON.stringify(next, null, 2) + "\n", "utf8");
  }

  return { ok: true, workId: wid, path: p.approvalRel, status: st, by: who, dry_run: !!dryRun, approval: next };
}

