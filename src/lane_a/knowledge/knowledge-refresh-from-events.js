import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { createHash } from "node:crypto";
import { validateKnowledgeChangeEvent } from "../../contracts/validators/index.js";
import { ensureLaneADirs, ensureKnowledgeDirs, loadProjectPaths } from "../../paths/project-paths.js";
import { loadRepoRegistry, resolveRepoAbsPath } from "../../utils/repo-registry.js";
import { runRepoIndex } from "./repo-indexer.js";
import { runKnowledgeScan } from "./knowledge-scan.js";

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

async function readJsonOptional(absPath) {
  const abs = resolve(String(absPath || ""));
  if (!existsSync(abs)) return { ok: true, exists: false, json: null };
  try {
    const t = await readFile(abs, "utf8");
    return { ok: true, exists: true, json: JSON.parse(String(t || "")) };
  } catch (err) {
    return { ok: false, exists: true, json: null, message: err instanceof Error ? err.message : String(err) };
  }
}

function segmentKeyFromFile(fileName) {
  const m = /^events-(\d{8}-\d{2})\.jsonl$/.exec(String(fileName || ""));
  return m ? m[1] : null;
}

function segmentFileForKey(key) {
  const k = normStr(key);
  if (!k) return null;
  return `events-${k}.jsonl`;
}

function normalizeCheckpoint(json) {
  const j = isPlainObject(json) ? json : {};
  if (j.version !== 1) return { ok: false, message: "checkpoint.version must be 1." };
  const last_processed_event_id = normStr(j.last_processed_event_id) || null;
  const last_processed_segment = normStr(j.last_processed_segment) || null;
  const updated_at = normStr(j.updated_at) || null;
  if (last_processed_event_id && !last_processed_segment) return { ok: false, message: "checkpoint.last_processed_segment is required when last_processed_event_id is set." };
  return { ok: true, checkpoint: { version: 1, last_processed_event_id, last_processed_segment, updated_at } };
}

async function loadCheckpoint({ checkpointAbs }) {
  const res = await readJsonOptional(checkpointAbs);
  if (!res.ok) throw new Error(`Invalid checkpoint (${checkpointAbs}): ${res.message}`);
  if (!res.exists) {
    return {
      ok: true,
      exists: false,
      checkpoint: { version: 1, last_processed_event_id: null, last_processed_segment: null, updated_at: null },
    };
  }
  const norm = normalizeCheckpoint(res.json);
  if (!norm.ok) throw new Error(`Invalid checkpoint (${checkpointAbs}): ${norm.message}`);
  return { ok: true, exists: true, checkpoint: norm.checkpoint };
}

async function listSegmentFiles({ segmentsDirAbs }) {
  if (!existsSync(segmentsDirAbs)) return [];
  const entries = await readdir(segmentsDirAbs, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile() && /^events-\d{8}-\d{2}\.jsonl$/.test(e.name))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));
}

async function readEventsAfterCheckpoint({ segmentsDirAbs, checkpoint, maxEvents = null }) {
  const files = await listSegmentFiles({ segmentsDirAbs });

  const anchorSegKey = normStr(checkpoint?.last_processed_segment) || null;
  const anchorEvId = normStr(checkpoint?.last_processed_event_id) || null;
  const anchorFile = anchorSegKey ? segmentFileForKey(anchorSegKey) : null;

  let started = anchorFile == null;
  let anchorFound = anchorEvId == null;

  const seen = new Set();
  const warnings = [];
  const events = [];
  let last = null;

  for (const f of files) {
    if (!started) {
      if (f !== anchorFile) continue;
      started = true;
    }

    const abs = join(segmentsDirAbs, f);
    // eslint-disable-next-line no-await-in-loop
    const text = await readFile(abs, "utf8");
    const lines = String(text || "").split("\n");

    for (const rawLine of lines) {
      const line = String(rawLine || "").trim();
      if (!line) continue;

      const obj = JSON.parse(line);
      validateKnowledgeChangeEvent(obj);

      if (!anchorFound) {
        if (String(obj.event_id) === anchorEvId) anchorFound = true;
        continue;
      }

      const id = String(obj.event_id);
      if (seen.has(id)) {
        warnings.push(`duplicate event_id encountered in segments: ${id}`);
        continue;
      }
      seen.add(id);

      const segKey = segmentKeyFromFile(f);
      if (!segKey) throw new Error(`Unexpected segment filename: ${f}`);
      events.push({ event: obj, segment_key: segKey, segment_file: f });
      last = { event_id: id, segment_key: segKey };

      if (typeof maxEvents === "number" && Number.isFinite(maxEvents) && maxEvents >= 0 && events.length >= maxEvents) break;
    }

    if (typeof maxEvents === "number" && Number.isFinite(maxEvents) && maxEvents >= 0 && events.length >= maxEvents) break;
  }

  if (anchorFile && started && !anchorFound) throw new Error(`Checkpoint anchor event_id not found in segment ${anchorFile}: ${anchorEvId}`);
  if (anchorFile && !started) throw new Error(`Checkpoint segment not found: ${anchorFile}`);

  return { ok: true, events, last, warnings };
}

async function markCommitteeStale({ paths, repoId, capturedAt, lastEventId, reason }) {
  const dirAbs = join(paths.knowledge.ssotReposAbs, repoId, "committee");
  const statusAbs = join(dirAbs, "committee_status.json");
  if (!existsSync(statusAbs)) return { ok: true, wrote: false };
  const staleAbs = join(dirAbs, "STALE.json");
  const obj = {
    version: 1,
    repo_id: repoId,
    captured_at: capturedAt,
    reason: normStr(reason) || "evidence_changed",
    last_event_id: normStr(lastEventId) || null,
  };
  await writeTextAtomic(staleAbs, JSON.stringify(obj, null, 2) + "\n");

  const integDirAbs = join(paths.knowledge.ssotSystemAbs, "committee", "integration");
  const integStatusAbs = join(integDirAbs, "integration_status.json");
  if (existsSync(integStatusAbs)) {
    const iStaleAbs = join(integDirAbs, "STALE.json");
    await writeTextAtomic(
      iStaleAbs,
      JSON.stringify(
        {
          version: 1,
          scope: "system",
          captured_at: capturedAt,
          reason: `repo_committee_stale:${repoId}`,
          last_event_id: normStr(lastEventId) || null,
        },
        null,
        2,
      ) + "\n",
    );
  }

  return { ok: true, wrote: true, path: staleAbs };
}

async function renderRefreshReportMd(report) {
  const lines = [];
  lines.push("KNOWLEDGE REFRESH FROM EVENTS");
  lines.push("");
  lines.push(`run_at: ${report.run_at}`);
  lines.push(`processed_events: ${report.processed_events}`);
  lines.push(`repos_impacted: ${(Array.isArray(report.repos_impacted) ? report.repos_impacted : []).join(", ") || "-"}`);
  lines.push("");
  lines.push("UPDATES");
  lines.push("");
  const upd = isPlainObject(report.updates) ? report.updates : {};
  const repoIds = Object.keys(upd).sort((a, b) => a.localeCompare(b));
  if (!repoIds.length) lines.push("- (none)");
  for (const repoId of repoIds) {
    const u = upd[repoId];
    lines.push(`- ${repoId}: status=${u.status} scan_actions=${u.scan_actions}`);
  }
  lines.push("");
  if (Array.isArray(report.warnings) && report.warnings.length) {
    lines.push("WARNINGS");
    lines.push("");
    for (const w of report.warnings) lines.push(`- ${w}`);
    lines.push("");
  }
  if (Array.isArray(report.errors) && report.errors.length) {
    lines.push("ERRORS");
    lines.push("");
    for (const e of report.errors) lines.push(`- ${e}`);
    lines.push("");
  }
  return lines.join("\n") + "\n";
}

async function writeRefreshReport({ paths, report, dryRun }) {
  const jsonAbs = join(paths.laneA.logsAbs, "knowledge-refresh-from-events.report.json");
  const mdAbs = join(paths.laneA.logsAbs, "knowledge-refresh-from-events.report.md");
  if (dryRun) return { ok: true, paths: { json: jsonAbs, md: mdAbs } };
  await mkdir(paths.laneA.logsAbs, { recursive: true });
  await writeTextAtomic(jsonAbs, JSON.stringify(report, null, 2) + "\n");
  await writeTextAtomic(mdAbs, await renderRefreshReportMd(report));
  return { ok: true, paths: { json: jsonAbs, md: mdAbs } };
}

async function writeCheckpoint({ checkpointAbs, last, dryRun }) {
  if (dryRun) return { ok: true, wrote: false };
  const next = {
    version: 1,
    last_processed_event_id: last?.event_id || null,
    last_processed_segment: last?.segment_key || null,
    updated_at: nowISO(),
  };
  await writeTextAtomic(checkpointAbs, JSON.stringify(next, null, 2) + "\n");
  return { ok: true, wrote: true, checkpoint: next };
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

async function writeKnowledgeEventsSummaryIfChanged({ paths, dryRun }) {
  const idxAbs = join(paths.laneA.eventsAbs, "index.json");
  const idxRes = await readJsonOptional(idxAbs);
  const idxJson = idxRes.ok && idxRes.exists ? idxRes.json : null;
  const idxNorm = idxJson && typeof idxJson === "object" ? idxJson : { version: 1, segments: [], events_total: 0, latest_event_at: null, active_segment: null };
  const idxHash = sha256Hex(JSON.stringify(idxNorm));

  const summaryAbs = paths.knowledge.eventsSummaryAbs;
  const prevRes = await readJsonOptional(summaryAbs);
  const prev = prevRes.ok && prevRes.exists && prevRes.json && typeof prevRes.json === "object" ? prevRes.json : null;
  if (prev && prev.source && prev.source.index_hash === idxHash) {
    return { ok: true, wrote: false, reason: "unchanged" };
  }
  if (dryRun) return { ok: true, wrote: false, dry_run: true };

  const segmentsDirAbs = paths.laneA.eventsSegmentsAbs;
  const segFiles = await listSegmentFiles({ segmentsDirAbs });
  const byType = {};
  const byRepo = {};
  const byScope = {};
  const events = [];

  for (const f of segFiles) {
    const abs = join(segmentsDirAbs, f);
    // eslint-disable-next-line no-await-in-loop
    const text = await readFile(abs, "utf8");
    const lines = String(text || "").split("\n").map((l) => l.trim()).filter(Boolean);
    for (const l of lines) {
      const obj = JSON.parse(l);
      validateKnowledgeChangeEvent(obj);
      const type = normStr(obj.type) || "unknown";
      const repoId = obj.repo_id ? normStr(obj.repo_id) : null;
      const scope = normStr(obj.scope) || "unknown";
      byType[type] = (byType[type] || 0) + 1;
      byScope[scope] = (byScope[scope] || 0) + 1;
      if (repoId) byRepo[repoId] = (byRepo[repoId] || 0) + 1;
      events.push({
        event_id: String(obj.event_id),
        timestamp: String(obj.timestamp),
        type,
        scope,
        repo_id: repoId,
        work_id: normStr(obj.work_id) || null,
        pr_number: obj.pr_number ?? null,
        commit: normStr(obj.commit) || null,
        summary: normStr(obj.summary) || null,
        artifacts: {
          paths: Array.isArray(obj?.artifacts?.paths) ? obj.artifacts.paths.map((p) => normStr(p)).filter(Boolean).sort((a, b) => a.localeCompare(b)) : [],
        },
      });
    }
  }

  events.sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)) || String(a.event_id).localeCompare(String(b.event_id)));
  const recentN = 50;
  const recent_events = events.slice(-recentN);

  const summary = {
    version: 1,
    generated_at: nowISO(),
    source: {
      ops_events_root: paths.laneA.eventsAbs,
      index_hash: idxHash,
    },
    events_total: events.length,
    latest_event_at: events.length ? events[events.length - 1].timestamp : null,
    by_type: Object.fromEntries(Object.entries(byType).sort((a, b) => a[0].localeCompare(b[0]))),
    by_scope: Object.fromEntries(Object.entries(byScope).sort((a, b) => a[0].localeCompare(b[0]))),
    by_repo: Object.fromEntries(Object.entries(byRepo).sort((a, b) => a[0].localeCompare(b[0]))),
    recent_events,
  };

  await writeTextAtomic(summaryAbs, JSON.stringify(summary, null, 2) + "\n");
  return { ok: true, wrote: true, path: summaryAbs };
}

export async function runRefreshFromEvents(projectRoot, options = {}) {
  const dryRun = options?.dryRun === true;
  const maxEvents = typeof options?.maxEvents === "number" && Number.isFinite(options.maxEvents) ? Math.max(0, Math.floor(options.maxEvents)) : null;
  const stopOnError = options?.stopOnError === true;

  const paths = await loadProjectPaths({ projectRoot });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, message: reposRes.message };
  const registry = reposRes.registry;
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  const byId = new Map(repos.map((r) => [normStr(r?.repo_id), r]));

  const checkpointAbs = join(paths.laneA.eventsCheckpointsAbs, "last_refresh.json");
  const cp = await loadCheckpoint({ checkpointAbs });

  const readRes = await readEventsAfterCheckpoint({
    segmentsDirAbs: paths.laneA.eventsSegmentsAbs,
    checkpoint: cp.checkpoint,
    maxEvents,
  });

  const capturedAt = nowISO();
  const warnings = readRes.warnings.slice();
  const errors = [];

  const impacted = [];
  const updates = {};
  const addUpdate = (repoId, patch) => {
    updates[repoId] = { ...(updates[repoId] || {}), ...patch };
  };

  const impactedSet = new Set();
  for (const { event } of readRes.events) {
    const repoId = event.repo_id ? normStr(event.repo_id) : null;
    if (!repoId) continue;
    impactedSet.add(repoId);
  }
  impacted.push(...Array.from(impactedSet).sort((a, b) => a.localeCompare(b)));

  for (const repoId of impacted) {
    const cfg = byId.get(repoId) || null;
    if (!cfg) {
      errors.push(`Unknown repo_id referenced by events: ${repoId}`);
      addUpdate(repoId, { status: "error", scan_actions: "full", fingerprint_changes: [], evidence_changes: [] });
      if (stopOnError) break;
      continue;
    }

    const repoAbs = resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: cfg.path });
    if (!repoAbs || !existsSync(repoAbs)) {
      errors.push(`Repo path missing for ${repoId}: ${repoAbs || "(null)"}`);
      addUpdate(repoId, { status: "error", scan_actions: "full", fingerprint_changes: [], evidence_changes: [] });
      if (stopOnError) break;
      continue;
    }

    try {
      if (!dryRun) {
        const outDir = join(paths.knowledge.evidenceIndexReposAbs, repoId);
        const idxRes = await runRepoIndex({
          repo_id: repoId,
          repo_path: repoAbs,
          output_dir: outDir,
          error_dir_abs: paths.laneA.logsAbs,
          repo_config: cfg,
          dry_run: false,
        });
        if (!idxRes.ok) throw new Error(idxRes.message || "index failed");

        const scanRes = await runKnowledgeScan({ projectRoot: paths.opsRootAbs, repoId, limit: 1, concurrency: 1, dryRun: false });
        if (!scanRes.ok) throw new Error(scanRes.failed && scanRes.failed[0] ? scanRes.failed[0].message : "scan failed");
      }

      const lastEventId = readRes.last ? readRes.last.event_id : null;
      await markCommitteeStale({ paths, repoId, capturedAt, lastEventId, reason: "refresh_from_events" });
      addUpdate(repoId, { status: "ok", scan_actions: "full", fingerprint_changes: [], evidence_changes: ["index_updated", "scan_updated"] });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`repo ${repoId}: refresh failed (${msg})`);
      addUpdate(repoId, { status: "error", scan_actions: "full", fingerprint_changes: [], evidence_changes: [] });
      if (stopOnError) break;
    }
  }

  const report = {
    version: 1,
    run_at: capturedAt,
    processed_events: readRes.events.length,
    repos_impacted: impacted,
    updates,
    next_checkpoint: readRes.last ? `${readRes.last.segment_key}:${readRes.last.event_id}` : null,
    errors,
    warnings,
  };

  await writeRefreshReport({ paths, report, dryRun });
  await writeKnowledgeEventsSummaryIfChanged({ paths, dryRun });

  if (errors.length) {
    return { ok: false, report, checkpoint: cp.checkpoint, message: "refresh encountered errors" };
  }

  if (!readRes.last) return { ok: true, report, checkpoint: cp.checkpoint };

  const wrote = await writeCheckpoint({ checkpointAbs, last: readRes.last, dryRun });
  return { ok: true, report, checkpoint: wrote.checkpoint || cp.checkpoint };
}
