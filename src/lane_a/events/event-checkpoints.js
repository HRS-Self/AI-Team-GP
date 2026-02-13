import { existsSync } from "node:fs";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function nowISO() {
  return new Date().toISOString();
}

let atomicCounter = 0;
async function writeTextAtomic(absPath, text) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  atomicCounter += 1;
  const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
  await writeFile(tmp, String(text || ""), "utf8");
  await rename(tmp, abs);
}

function checkpointFileName(consumer) {
  const c = normStr(consumer);
  if (!c) throw new Error("checkpoint consumer is required.");
  if (!/^[a-z0-9][a-z0-9_-]{0,63}$/i.test(c)) throw new Error(`Invalid checkpoint consumer name '${c}'.`);
  return `consumer-${c}.json`;
}

function normalizeCheckpoint(json, consumer) {
  const j = isPlainObject(json) ? json : {};
  if (j.version !== 1) throw new Error("checkpoint.version must be 1.");
  const c = normStr(j.consumer) || normStr(consumer);
  if (!c) throw new Error("checkpoint.consumer is required.");
  const last_read_segment = normStr(j.last_read_segment) || null;
  const last_read_offset = j.last_read_offset == null ? null : Number(j.last_read_offset);
  if (last_read_segment && (last_read_offset == null || !Number.isFinite(last_read_offset) || last_read_offset < 0)) {
    throw new Error("checkpoint.last_read_offset must be a non-negative number when last_read_segment is set.");
  }
  if (!last_read_segment) {
    // Brand new / reset checkpoint: allow null segment with offset 0 (start of stream).
    if (last_read_offset != null && Number.isFinite(last_read_offset) && Math.floor(last_read_offset) !== 0) {
      throw new Error("checkpoint.last_read_segment is required when last_read_offset is non-zero.");
    }
  }
  return {
    version: 1,
    consumer: c,
    last_read_segment,
    last_read_offset: last_read_offset == null ? 0 : Math.floor(last_read_offset),
    updated_at: normStr(j.updated_at) || null,
  };
}

export async function readCheckpoint({ checkpointsDirAbs, consumer }) {
  const dirAbs = resolve(String(checkpointsDirAbs || ""));
  const file = checkpointFileName(consumer);
  const abs = join(dirAbs, file);
  if (!existsSync(abs)) {
    return {
      ok: true,
      exists: false,
      checkpoint: normalizeCheckpoint({ version: 1, consumer, last_read_segment: null, last_read_offset: 0, updated_at: null }, consumer),
      path: abs,
    };
  }
  const text = await readFile(abs, "utf8");
  const json = JSON.parse(String(text || ""));
  return { ok: true, exists: true, checkpoint: normalizeCheckpoint(json, consumer), path: abs };
}

export async function writeCheckpoint({ checkpointsDirAbs, consumer, last_segment, last_offset, dryRun = false }) {
  const dirAbs = resolve(String(checkpointsDirAbs || ""));
  const file = checkpointFileName(consumer);
  const abs = join(dirAbs, file);
  const next = normalizeCheckpoint(
    {
      version: 1,
      consumer,
      last_read_segment: normStr(last_segment) || null,
      last_read_offset: last_offset == null ? 0 : Number(last_offset),
      updated_at: nowISO(),
    },
    consumer,
  );
  if (dryRun) return { ok: true, wrote: false, path: abs, checkpoint: next };
  await writeTextAtomic(abs, JSON.stringify(next, null, 2) + "\n");
  return { ok: true, wrote: true, path: abs, checkpoint: next };
}
