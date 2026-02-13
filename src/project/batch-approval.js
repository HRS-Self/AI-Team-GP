import { readTextIfExists, writeText, ensureDir } from "../utils/fs.js";

function nowISO() {
  return new Date().toISOString();
}

export function normalizeRawIntakeId(input) {
  const s = String(input || "").trim();
  if (!s) return null;
  return s.replace(/\.md$/i, "");
}

export function batchIdForRawIntake(rawIntakeId) {
  const id = normalizeRawIntakeId(rawIntakeId);
  return id ? `BATCH-${id}` : null;
}

export function batchApprovalPath(rawIntakeId) {
  const id = normalizeRawIntakeId(rawIntakeId);
  if (!id) return null;
  return `ai/lane_b/approvals/BATCH-${id}.json`;
}

export async function readBatchApproval(rawIntakeId) {
  const p = batchApprovalPath(rawIntakeId);
  if (!p) return { ok: false, message: "rawIntakeId missing" };
  const t = await readTextIfExists(p);
  if (!t) return { ok: true, exists: false, approval: null, path: p };
  try {
    const json = JSON.parse(t);
    return { ok: true, exists: true, approval: json, path: p };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Invalid batch approval JSON (${msg})`, path: p };
  }
}

export async function writeBatchApproval({ rawIntakeId, status, notes = "", approvedBy = "human" }) {
  const id = normalizeRawIntakeId(rawIntakeId);
  if (!id) return { ok: false, message: "Missing --intake I-<...>." };
  if (!(status === "approved" || status === "rejected" || status === "pending")) return { ok: false, message: "status must be approved|rejected|pending." };

  await ensureDir("ai/lane_b/approvals");
  const path = `ai/lane_b/approvals/BATCH-${id}.json`;
  const obj = {
    version: 1,
    batch_id: `BATCH-${id}`,
    raw_intake_id: id,
    status,
    notes: String(notes || ""),
    approved_at: status === "pending" ? null : nowISO(),
    approved_by: approvedBy,
  };
  await writeText(path, JSON.stringify(obj, null, 2) + "\n");
  return { ok: true, path, approval: obj };
}
