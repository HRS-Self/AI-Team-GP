import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, rename, stat, unlink, writeFile, appendFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { isAbsolute } from "node:path";

import { validateKnowledgeChangeEvent } from "../../contracts/validators/index.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function nowISO() {
  return new Date().toISOString();
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function utcSegmentKey(d) {
  const dt = d instanceof Date ? d : new Date();
  return `${dt.getUTCFullYear()}${pad2(dt.getUTCMonth() + 1)}${pad2(dt.getUTCDate())}-${pad2(dt.getUTCHours())}`;
}

function stableEventId({ type, repo_id, commit, paths }) {
  const parts = [String(type || ""), String(repo_id ?? ""), String(commit || ""), ...(Array.isArray(paths) ? paths : [])];
  const base = parts.map((p) => String(p ?? "")).join("\n");
  const h = createHash("sha256").update(base, "utf8").digest("hex").slice(0, 16);
  return `KEVT_${h}`;
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

async function readJsonOptional(absPath) {
  if (!existsSync(absPath)) return { ok: true, exists: false, json: null };
  try {
    const t = await readFile(absPath, "utf8");
    return { ok: true, exists: true, json: JSON.parse(String(t || "")) };
  } catch (err) {
    return { ok: false, exists: true, json: null, message: err instanceof Error ? err.message : String(err) };
  }
}

function normalizeIndex(json) {
  const j = json && typeof json === "object" ? json : {};
  const segments = Array.isArray(j.segments) ? j.segments : [];
  return {
    version: 1,
    updated_at: typeof j.updated_at === "string" ? j.updated_at : null,
    active_segment: typeof j.active_segment === "string" ? j.active_segment : null,
    events_total: typeof j.events_total === "number" ? j.events_total : 0,
    latest_event_at: typeof j.latest_event_at === "string" ? j.latest_event_at : null,
    segments: segments
      .filter((s) => s && typeof s === "object" && typeof s.file === "string")
      .map((s) => ({
        file: String(s.file),
        created_at: typeof s.created_at === "string" ? s.created_at : null,
        latest_event_at: typeof s.latest_event_at === "string" ? s.latest_event_at : null,
        events: typeof s.events === "number" ? s.events : 0,
      }))
      .sort((a, b) => a.file.localeCompare(b.file)),
  };
}

function eventsRootAbs(opsLaneAAbs) {
  return join(resolve(String(opsLaneAAbs || "")), "events");
}

function requireOpsLaneAAbs(opsLaneAAbs) {
  const raw = normStr(opsLaneAAbs);
  if (!raw) throw new Error("Missing opsLaneAAbs.");
  if (!isAbsolute(raw)) throw new Error(`opsLaneAAbs must be an absolute path (got: ${raw}).`);
  return resolve(raw);
}

async function ensureLayout({ opsLaneAAbs }) {
  const laneAAbs = requireOpsLaneAAbs(opsLaneAAbs);
  const root = eventsRootAbs(laneAAbs);
  const segmentsDir = join(root, "segments");
  const checkpointsDir = join(root, "checkpoints");
  await mkdir(segmentsDir, { recursive: true });
  await mkdir(checkpointsDir, { recursive: true });
  return { root, segmentsDir, checkpointsDir };
}

async function loadIndex({ opsLaneAAbs }) {
  const { root } = await ensureLayout({ opsLaneAAbs });
  const idxAbs = join(root, "index.json");
  const res = await readJsonOptional(idxAbs);
  if (!res.ok) throw new Error(`Invalid knowledge events index.json (${idxAbs}): ${res.message}`);
  const idx = normalizeIndex(res.exists ? res.json : null);
  return { idxAbs, idx };
}

async function persistIndex({ idxAbs, idx }) {
  const next = normalizeIndex(idx);
  next.updated_at = nowISO();
  await writeTextAtomic(idxAbs, JSON.stringify(next, null, 2) + "\n");
  return next;
}

export async function rotateIfNeeded(now, { opsLaneAAbs, maxSegmentBytes = 1024 * 1024 } = {}) {
  const laneA = requireOpsLaneAAbs(opsLaneAAbs);
  const { root, segmentsDir } = await ensureLayout({ opsLaneAAbs: laneA });
  const { idxAbs, idx } = await loadIndex({ opsLaneAAbs: laneA });

  const key = utcSegmentKey(now || new Date());
  const desired = `events-${key}.jsonl`;
  const current = idx.active_segment;

  let rotate = current !== desired;
  if (!rotate && current) {
    const abs = join(segmentsDir, current);
    try {
      const st = await stat(abs);
      if (Number.isFinite(Number(maxSegmentBytes)) && Number(maxSegmentBytes) > 0 && st.size > Number(maxSegmentBytes)) rotate = true;
    } catch {
      rotate = true;
    }
  }

  if (!rotate) return { ok: true, root, segmentsDir, active_segment: current, index: idx };

  const activeAbs = join(segmentsDir, desired);
  if (!existsSync(activeAbs)) await writeFile(activeAbs, "", "utf8");
  const next = { ...idx };
  next.active_segment = desired;
  if (!next.segments.some((s) => s.file === desired)) {
    next.segments = next.segments.concat([{ file: desired, created_at: nowISO(), latest_event_at: null, events: 0 }]).sort((a, b) => a.file.localeCompare(b.file));
  }
  const persisted = await persistIndex({ idxAbs, idx: next });
  return { ok: true, root, segmentsDir, active_segment: desired, index: persisted };
}

export async function appendEvent(event, { opsLaneAAbs, now = null, maxSegmentBytes = 1024 * 1024, dryRun = false } = {}) {
  const laneA = requireOpsLaneAAbs(opsLaneAAbs);

  const ts = now instanceof Date ? now : new Date();
  const rotated = await rotateIfNeeded(ts, { opsLaneAAbs: laneA, maxSegmentBytes });
  const { root, segmentsDir, active_segment } = rotated;
  const { idxAbs, idx } = await loadIndex({ opsLaneAAbs: laneA });

  const base = event && typeof event === "object" ? { ...event } : {};
  const type = normStr(base.type);
  const scope = normStr(base.scope);
  const repo_id = base.repo_id === null ? null : normStr(base.repo_id);
  const commit = normStr(base.commit);
  const paths = Array.isArray(base?.artifacts?.paths) ? base.artifacts.paths.map((p) => normStr(p)).filter(Boolean).sort((a, b) => a.localeCompare(b)) : [];

  const computedId = stableEventId({ type, repo_id, commit, paths });
  const event_id = normStr(base.event_id) || computedId;
  if (event_id !== computedId) throw new Error(`event_id mismatch (expected ${computedId}, got ${event_id}).`);

  const line = {
    version: 1,
    event_id,
    type,
    scope,
    repo_id: repo_id || null,
    work_id: normStr(base.work_id),
    pr_number: base.pr_number === null || base.pr_number === undefined ? null : Number(base.pr_number),
    commit,
    artifacts: {
      paths,
      fingerprints: Array.isArray(base?.artifacts?.fingerprints)
        ? base.artifacts.fingerprints.map((x) => normStr(x)).filter(Boolean).sort((a, b) => a.localeCompare(b))
        : [],
    },
    summary: normStr(base.summary),
    timestamp: typeof base.timestamp === "string" && base.timestamp.trim() ? base.timestamp.trim() : ts.toISOString(),
  };

  validateKnowledgeChangeEvent(line);

  const segAbs = join(segmentsDir, String(active_segment || ""));
  const appendText = JSON.stringify(line) + "\n";

  if (!dryRun) {
    await mkdir(dirname(segAbs), { recursive: true });
    await appendFile(segAbs, appendText, "utf8");

    // Update index counters deterministically.
    const next = normalizeIndex(idx);
    next.events_total = (Number.isFinite(next.events_total) ? next.events_total : 0) + 1;
    next.latest_event_at = line.timestamp;
    next.active_segment = active_segment;
    next.segments = next.segments.map((s) => {
      if (s.file !== active_segment) return s;
      return { ...s, latest_event_at: line.timestamp, events: (Number.isFinite(s.events) ? s.events : 0) + 1 };
    });
    await persistIndex({ idxAbs, idx: next });
  }

  return { ok: true, event: line, segment: `segments/${active_segment}`, ops_events_root: root };
}

export async function readEventsSince(checkpoint, { opsLaneAAbs } = {}) {
  const laneA = requireOpsLaneAAbs(opsLaneAAbs);
  const root = eventsRootAbs(laneA);
  const segmentsDir = join(root, "segments");
  const sinceTs = checkpoint && typeof checkpoint === "object" && typeof checkpoint.timestamp === "string" ? checkpoint.timestamp : null;
  const sinceMs = sinceTs ? Date.parse(sinceTs) : null;

  const entries = existsSync(segmentsDir) ? await readdir(segmentsDir, { withFileTypes: true }) : [];
  const files = entries
    .filter((e) => e.isFile() && e.name.startsWith("events-") && e.name.endsWith(".jsonl"))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));

  const events = [];
  for (const f of files) {
    const abs = join(segmentsDir, f);
    // eslint-disable-next-line no-await-in-loop
    const text = await readFile(abs, "utf8");
    const lines = String(text || "")
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);
    for (const l of lines) {
      const obj = JSON.parse(l);
      validateKnowledgeChangeEvent(obj);
      const ms = Date.parse(String(obj.timestamp));
      if (Number.isFinite(Number(sinceMs)) && Number.isFinite(ms) && ms <= sinceMs) continue;
      events.push(obj);
    }
  }

  events.sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)) || String(a.event_id).localeCompare(String(b.event_id)));
  return { ok: true, events };
}

export async function compactOlderThan(days, { opsLaneAAbs } = {}) {
  const laneA = resolve(String(opsLaneAAbs || ""));
  if (!laneA) throw new Error("Missing opsLaneAAbs.");
  const d = Number.isFinite(Number(days)) ? Math.max(0, Math.floor(Number(days))) : 0;
  const cutoffMs = Date.now() - d * 24 * 60 * 60 * 1000;

  const { root, segmentsDir, checkpointsDir } = await ensureLayout({ opsLaneAAbs: laneA });
  const { idxAbs, idx } = await loadIndex({ opsLaneAAbs: laneA });
  const active = idx.active_segment;

  const entries = await readdir(segmentsDir, { withFileTypes: true });
  const files = entries
    .filter((e) => e.isFile() && e.name.startsWith("events-") && e.name.endsWith(".jsonl") && e.name !== active)
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));

  const toCompact = [];
  for (const f of files) {
    const abs = join(segmentsDir, f);
    // eslint-disable-next-line no-await-in-loop
    const st = await stat(abs);
    if (st.mtimeMs < cutoffMs) toCompact.push({ file: f, abs });
  }
  if (!toCompact.length) return { ok: true, compacted: 0 };

  let eventsCompacted = 0;
  let latestEventAt = null;
  for (const seg of toCompact) {
    // eslint-disable-next-line no-await-in-loop
    const text = await readFile(seg.abs, "utf8");
    const lines = String(text || "")
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);
    for (const l of lines) {
      const obj = JSON.parse(l);
      validateKnowledgeChangeEvent(obj);
      eventsCompacted += 1;
      latestEventAt = String(obj.timestamp);
    }
  }

  const checkpoint = {
    version: 1,
    compacted_at: nowISO(),
    through_segment: toCompact.map((s) => s.file).sort((a, b) => a.localeCompare(b)).at(-1),
    events_compacted: eventsCompacted,
    latest_event_at: latestEventAt,
  };

  await writeTextAtomic(join(checkpointsDir, "last_compacted.json"), JSON.stringify(checkpoint, null, 2) + "\n");

  for (const seg of toCompact) {
    // eslint-disable-next-line no-await-in-loop
    await unlink(seg.abs);
  }

  const remaining = idx.segments.filter((s) => !toCompact.some((c) => c.file === s.file));
  const next = { ...idx, segments: remaining };
  await persistIndex({ idxAbs, idx: next });

  return { ok: true, compacted: toCompact.length, events_compacted: eventsCompacted, checkpoint };
}
