import { formatFsSafeUtcTimestamp, makeId, nowFsSafeUtcTimestamp, shortHash } from "./naming.js";

export function todayUtcYyyyMmDd(date = null) {
  const d = date instanceof Date ? date : date ? new Date(date) : new Date();
  const yyyy = String(d.getUTCFullYear()).padStart(4, "0");
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

// Canonical timestamp for artifacts/ledger fields (symbol-safe, UTC).
export function nowTs() {
  return nowFsSafeUtcTimestamp();
}

// Canonical timestamp for filenames/IDs (same format).
export { formatFsSafeUtcTimestamp };

export function intakeId({ timestamp = null, text = "" } = {}) {
  return makeId("I", { timestamp, seed: String(text || "") });
}

export function triagedId({ timestamp = null, seed = "" } = {}) {
  return makeId("T", { timestamp, seed: String(seed || "") });
}

export function batchIdFromRawIntakeId(rawIntakeId) {
  const id = String(rawIntakeId || "").trim();
  if (!id) throw new Error("Missing raw_intake_id for batch id.");
  return `BATCH-${id}`;
}

export function workId({ timestamp = null, seed = "" } = {}) {
  return makeId("W", { timestamp, seed: String(seed || "") });
}

export function shortIdFromText(text, len = 6) {
  return shortHash(String(text || ""), len);
}

