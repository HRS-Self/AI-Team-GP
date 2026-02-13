import { open, stat, unlink, writeFile } from "node:fs/promises";
import os from "node:os";
import { resolveStatePath } from "../project/state-paths.js";

function nowISO() {
  return new Date().toISOString();
}

async function writeLockFile(path, meta) {
  await writeFile(path, JSON.stringify(meta, null, 2) + "\n", "utf8");
}

export async function acquireLock({ path, staleMs = 30 * 60 * 1000, metadata = null }) {
  const resolved = resolveStatePath(path);
  const lockMeta = {
    pid: process.pid,
    hostname: os.hostname(),
    started_at: nowISO(),
    ...(metadata && typeof metadata === "object" ? { metadata } : {}),
  };

  let staleReplaced = false;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const fh = await open(resolved, "wx");
      try {
        await writeLockFile(resolved, lockMeta);
      } finally {
        await fh.close();
      }
      return { ok: true, acquired: true, stale_replaced: staleReplaced, path: resolved };
    } catch (err) {
      if (!err || err.code !== "EEXIST") throw err;
      let info = null;
      try {
        info = await stat(resolved);
      } catch {
        info = null;
      }
      if (!info) continue;
      const ageMs = Date.now() - info.mtimeMs;
      if (ageMs > staleMs) {
        try {
          await unlink(resolved);
          staleReplaced = true;
          continue;
        } catch {
          return { ok: false, acquired: false, stale_replaced: staleReplaced, reason: "stale_replace_failed", path: resolved };
        }
      }
      return { ok: false, acquired: false, stale_replaced: false, reason: "locked", path: resolved };
    }
  }
  return { ok: false, acquired: false, stale_replaced: staleReplaced, reason: "locked", path: resolved };
}

export async function releaseLock(path) {
  const resolved = resolveStatePath(path);
  try {
    await unlink(resolved);
    return { ok: true };
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return { ok: true };
    return { ok: false, error: err };
  }
}
