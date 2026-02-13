import { randomBytes } from "node:crypto";
import os from "node:os";
import { mkdir, open, readFile, rename, stat, unlink } from "node:fs/promises";
import { dirname, resolve } from "node:path";

function fsSafeTimestamp(date = new Date()) {
  const iso = date instanceof Date ? date.toISOString() : new Date(date).toISOString();
  return iso.replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z").replace("T", "_");
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function normalizeTtlMs(ttlMs) {
  const parsed = Number(ttlMs);
  if (Number.isFinite(parsed) && parsed > 0) return Math.floor(parsed);
  return 8 * 60 * 1000;
}

function parseIsoMs(text) {
  const s = normStr(text);
  if (!s) return null;
  const ms = Date.parse(s);
  return Number.isFinite(ms) ? ms : null;
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function buildOwnerToken(owner = null) {
  const explicit = normStr(owner?.owner_token);
  if (explicit && /^[0-9a-fA-F]{32,}$/.test(explicit)) return explicit.toLowerCase();
  return randomBytes(16).toString("hex");
}

function normalizeOwner(owner = null) {
  const pidRaw = owner?.pid;
  const pid = Number.isInteger(pidRaw) ? pidRaw : Number.isInteger(Number(pidRaw)) ? Number(pidRaw) : null;

  const uidRaw = owner?.uid;
  let uid = null;
  if (typeof uidRaw === "string" && uidRaw.trim()) uid = uidRaw.trim();
  else if (Number.isInteger(uidRaw)) uid = String(uidRaw);
  else if (Number.isInteger(Number(uidRaw))) uid = String(Number(uidRaw));

  return {
    pid,
    uid,
    user: normStr(owner?.user) || null,
    host: normStr(owner?.host) || os.hostname(),
    cwd: normStr(owner?.cwd) || process.cwd(),
    command: normStr(owner?.command) || process.argv.join(" "),
    project_root: normStr(owner?.project_root),
    ai_project_root: normStr(owner?.ai_project_root),
  };
}

function buildLockRecord({ ttlMs, owner }) {
  const createdAtMs = Date.now();
  const createdAt = new Date(createdAtMs).toISOString();
  const expiresAt = new Date(createdAtMs + normalizeTtlMs(ttlMs)).toISOString();
  const o = normalizeOwner(owner);

  return {
    version: 1,
    lock_name: "lane-a-orchestrate",
    created_at: createdAt,
    expires_at: expiresAt,
    pid: o.pid,
    uid: o.uid,
    user: o.user,
    host: o.host,
    cwd: o.cwd,
    command: o.command,
    project_root: o.project_root || null,
    ai_project_root: o.ai_project_root || null,
    owner_token: buildOwnerToken(owner),
  };
}

async function readTextIfExists(absPath) {
  try {
    return await readFile(absPath, "utf8");
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return null;
    throw err;
  }
}

async function statIfExists(absPath) {
  try {
    return await stat(absPath);
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return null;
    throw err;
  }
}

function parseLockRecordBestEffort(text) {
  try {
    const parsed = JSON.parse(String(text || ""));
    return isPlainObject(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function lockIsStale(lock, { ttlMs, nowMs }) {
  const expiresAtMs = parseIsoMs(lock?.expires_at);
  if (expiresAtMs != null && nowMs >= expiresAtMs) return true;
  const createdAtMs = parseIsoMs(lock?.created_at);
  if (createdAtMs != null && nowMs - createdAtMs > ttlMs) return true;
  return false;
}

async function renameAsStale(absLockPath) {
  const stalePath = `${absLockPath}.stale-${fsSafeTimestamp()}-${process.pid}.json`;
  await rename(absLockPath, stalePath);
  return stalePath;
}

async function writeLockFile(fh, record) {
  await fh.writeFile(`${JSON.stringify(record, null, 2)}\n`, "utf8");
}

function lockError(prefix, err) {
  const msg = err instanceof Error ? err.message : String(err);
  return `${prefix}: ${msg}`;
}

export async function readOpsLock({ lockPath }) {
  const absLockPath = resolve(String(lockPath || ""));
  try {
    const text = await readTextIfExists(absLockPath);
    if (!text) return { ok: true, exists: false, lock: null };
    const lock = parseLockRecordBestEffort(text);
    return { ok: true, exists: true, lock };
  } catch (err) {
    return { ok: false, error: lockError("readOpsLock failed", err) };
  }
}

export async function breakStaleOpsLock({ lockPath, ttlMs }) {
  const absLockPath = resolve(String(lockPath || ""));
  const ttl = normalizeTtlMs(ttlMs);
  try {
    const [text, st] = await Promise.all([readTextIfExists(absLockPath), statIfExists(absLockPath)]);
    if (!st) return { ok: true, broken: false, reason: "missing" };
    const lock = parseLockRecordBestEffort(text);
    let stale = false;

    if (lock) {
      stale = lockIsStale(lock, { ttlMs: ttl, nowMs: Date.now() });
    } else {
      stale = Date.now() - st.mtimeMs > ttl;
    }
    if (!stale) return { ok: true, broken: false, reason: "not_stale", previous: lock || null };

    await renameAsStale(absLockPath);
    return { ok: true, broken: true, previous: lock || null };
  } catch (err) {
    return { ok: false, error: lockError("breakStaleOpsLock failed", err) };
  }
}

export async function acquireOpsLock({ lockPath, ttlMs, owner }) {
  const absLockPath = resolve(String(lockPath || ""));
  const ttl = normalizeTtlMs(ttlMs);
  let brokeStale = false;

  try {
    await mkdir(dirname(absLockPath), { recursive: true });
  } catch (err) {
    return { ok: false, error: lockError("acquireOpsLock failed", err) };
  }

  for (let attempt = 0; attempt < 4; attempt += 1) {
    const record = buildLockRecord({ ttlMs: ttl, owner });
    try {
      const fh = await open(absLockPath, "wx");
      try {
        await writeLockFile(fh, record);
      } finally {
        await fh.close();
      }
      return { ok: true, acquired: true, lock: record, broke_stale: brokeStale };
    } catch (err) {
      if (!err || err.code !== "EEXIST") {
        return { ok: false, error: lockError("acquireOpsLock failed", err) };
      }
    }

    let existingText = null;
    let existingStat = null;
    try {
      existingText = await readTextIfExists(absLockPath);
      existingStat = await statIfExists(absLockPath);
    } catch (err) {
      return { ok: false, error: lockError("acquireOpsLock failed", err) };
    }

    if (!existingStat) {
      continue;
    }

    const current = parseLockRecordBestEffort(existingText);
    const nowMs = Date.now();
    const stale = current ? lockIsStale(current, { ttlMs: ttl, nowMs }) : nowMs - existingStat.mtimeMs > ttl;
    if (!stale) {
      return { ok: true, acquired: false, reason: "lock_held", lock: current || null };
    }

    try {
      await renameAsStale(absLockPath);
      brokeStale = true;
      continue;
    } catch (err) {
      if (err && err.code === "ENOENT") continue;
      return { ok: false, error: lockError("acquireOpsLock failed", err) };
    }
  }

  return { ok: true, acquired: false, reason: "lock_held", lock: null };
}

export async function releaseOpsLock({ lockPath, owner }) {
  const absLockPath = resolve(String(lockPath || ""));
  const ownerToken =
    normStr(owner?.owner_token) ||
    (typeof owner === "string" ? normStr(owner) : "") ||
    (typeof owner?.token === "string" ? normStr(owner.token) : "") ||
    (typeof owner?.ownerToken === "string" ? normStr(owner.ownerToken) : "");

  try {
    const text = await readTextIfExists(absLockPath);
    if (!text) return { ok: true, released: false, reason: "missing" };
    const lock = parseLockRecordBestEffort(text);
    const lockToken = normStr(lock?.owner_token);
    if (!ownerToken || !lockToken || ownerToken !== lockToken) {
      return { ok: true, released: false, reason: "not_owner" };
    }
    await unlink(absLockPath);
    return { ok: true, released: true };
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return { ok: true, released: false, reason: "missing" };
    return { ok: false, error: lockError("releaseOpsLock failed", err) };
  }
}
