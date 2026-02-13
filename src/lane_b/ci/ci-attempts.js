import { ensureDir, readTextIfExists, writeText } from "../../utils/fs.js";

export const DEFAULT_MAX_CI_FIX_ATTEMPTS = 5;
export const DEFAULT_MAX_CI_UNCHANGED_POLLS_IN_FIXING = 3;

function nowISO() {
  return new Date().toISOString();
}

function safeJsonParse(text, label) {
  try {
    return { ok: true, json: JSON.parse(String(text || "")) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Invalid JSON in ${label} (${msg}).` };
  }
}

function normalizeAttempts(doc, { workId }) {
  const wid = String(workId || "").trim();
  const d = doc && typeof doc === "object" ? doc : {};
  const fix_attempts = Number.isFinite(Number(d.fix_attempts)) ? Math.max(0, Math.floor(Number(d.fix_attempts))) : 0;
  const unchanged = Number.isFinite(Number(d.unchanged_polls_in_fixing)) ? Math.max(0, Math.floor(Number(d.unchanged_polls_in_fixing))) : 0;

  return {
    version: 1,
    workId: wid,
    fix_attempts,
    last_fix_attempt_at: typeof d.last_fix_attempt_at === "string" ? d.last_fix_attempt_at : null,
    last_fix_snapshot_hash: typeof d.last_fix_snapshot_hash === "string" ? d.last_fix_snapshot_hash : null,
    last_polled_snapshot_hash: typeof d.last_polled_snapshot_hash === "string" ? d.last_polled_snapshot_hash : null,
    unchanged_polls_in_fixing: unchanged,
    updated_at: nowISO(),
  };
}

export async function readCiAttempts({ workId }) {
  const wid = String(workId || "").trim();
  if (!wid) return { ok: false, message: "Missing workId." };
  const path = `ai/lane_b/work/${wid}/CI/attempts.json`;
  const text = await readTextIfExists(path);
  if (!text) return { ok: true, exists: false, path, attempts: normalizeAttempts(null, { workId: wid }) };
  const parsed = safeJsonParse(text, path);
  if (!parsed.ok) return { ok: false, message: parsed.message, path };
  return { ok: true, exists: true, path, attempts: normalizeAttempts(parsed.json, { workId: wid }) };
}

export async function writeCiAttempts({ workId, attempts }) {
  const wid = String(workId || "").trim();
  if (!wid) return { ok: false, message: "Missing workId." };
  const dir = `ai/lane_b/work/${wid}/CI`;
  await ensureDir(dir);
  const path = `${dir}/attempts.json`;
  const next = normalizeAttempts(attempts, { workId: wid });
  await writeText(path, JSON.stringify(next, null, 2) + "\n");
  return { ok: true, path, attempts: next };
}
