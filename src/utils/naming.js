import { createHash } from "node:crypto";

const SAFE_NAME_RE = /^[A-Za-z0-9_-]+$/;
const SAFE_REPO_ID_RE = /^[a-z0-9_-]+$/;
// Git branch segments may include '/' separators, but each segment must be symbol-safe.
const SAFE_BRANCH_RE = /^[A-Za-z0-9_/-]+$/;

export function formatFsSafeUtcTimestamp(date = null) {
  const d = date instanceof Date ? date : date ? new Date(date) : new Date();
  // UTC components.
  const yyyy = String(d.getUTCFullYear()).padStart(4, "0");
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const HH = String(d.getUTCHours()).padStart(2, "0");
  const MM = String(d.getUTCMinutes()).padStart(2, "0");
  const SS = String(d.getUTCSeconds()).padStart(2, "0");
  const mmm = String(d.getUTCMilliseconds()).padStart(3, "0");
  return `${yyyy}${mm}${dd}_${HH}${MM}${SS}${mmm}`;
}

export function nowFsSafeUtcTimestamp() {
  return formatFsSafeUtcTimestamp(new Date());
}

export function assertSymbolSafeName(name, { kind = "name" } = {}) {
  const n = String(name || "").trim();
  if (!n) throw new Error(`Invalid ${kind}: empty. Use formatFsSafeUtcTimestamp() for all generated names.`);
  if (!SAFE_NAME_RE.test(n)) {
    const bad = n.replace(/[A-Za-z0-9_-]/g, "").slice(0, 16);
    throw new Error(
      `Invalid ${kind}: contains forbidden character(s) ${JSON.stringify(bad)}. Allowed: [A-Za-z0-9_-]. Use formatFsSafeUtcTimestamp() for all generated names.`,
    );
  }
  return n;
}

export function assertSafeRepoId(repoId) {
  const r = String(repoId || "").trim();
  if (!r) throw new Error("Invalid repo_id: empty.");
  if (!SAFE_REPO_ID_RE.test(r)) {
    throw new Error(`Invalid repo_id '${r}': must match ${String(SAFE_REPO_ID_RE)} for branch naming.`);
  }
  return r;
}

export function assertSafeBranchName(branch) {
  const b = String(branch || "").trim();
  if (!b) throw new Error("Invalid branch name: empty.");
  if (!SAFE_BRANCH_RE.test(b)) {
    throw new Error(`Invalid branch name '${b}': allowed characters are [A-Za-z0-9_/-] only.`);
  }
  // Also ensure no empty segments (e.g. leading/trailing '/').
  for (const seg of b.split("/")) assertSymbolSafeName(seg, { kind: "branch segment" });
  return b;
}

export function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

export function shortHash(text, len = 6) {
  const l = Number(len);
  const n = Number.isFinite(l) && l > 0 ? Math.floor(l) : 6;
  return sha256Hex(String(text || "")).slice(0, n);
}

export function makeId(prefix, { timestamp = null, seed = "" } = {}) {
  const rawTs = timestamp;
  const ts =
    typeof rawTs === "string" && /^[0-9]{8}_[0-9]{9}$/.test(rawTs.trim())
      ? rawTs.trim()
      : rawTs
        ? formatFsSafeUtcTimestamp(rawTs)
        : nowFsSafeUtcTimestamp();
  const h = shortHash(`${prefix}:${ts}:${seed}`, 6);
  const id = `${String(prefix).trim()}-${ts}_${h}`;
  assertSymbolSafeName(id.replace(/^[A-Za-z]+-/, ""), { kind: `${prefix} id core` }); // ts_hash part
  return id;
}

export function workBranchName({ workId, repoId }) {
  // Branch policy: ai/<sanitized_work_id>/<repo_id>
  const wid = String(workId || "").trim();
  if (!wid) throw new Error("Missing workId for branch naming.");
  // workId itself must already be symbol-safe for new work.
  if (!/^W-[A-Za-z0-9_-]+$/.test(wid)) {
    throw new Error(
      `Invalid workId '${wid}' for branch naming. Work IDs must be generated using formatFsSafeUtcTimestamp() and contain only [A-Za-z0-9_-].`,
    );
  }
  const rid = assertSafeRepoId(repoId);
  const branch = `ai/${wid}/${rid}`;
  return assertSafeBranchName(branch);
}

export function isLegacyUnsafeName(name) {
  const n = String(name || "");
  return /[:.+\\s]/.test(n);
}
