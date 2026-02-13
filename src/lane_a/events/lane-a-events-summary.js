import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { loadProjectPaths } from "../../paths/project-paths.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function parseMs(iso) {
  const s = normStr(iso);
  if (!s) return null;
  const ms = Date.parse(s);
  return Number.isFinite(ms) ? ms : null;
}

function ensureInt(n) {
  const x = typeof n === "number" ? n : Number.parseInt(String(n ?? ""), 10);
  return Number.isFinite(x) ? x : null;
}

function validateMergeEventShape(ev) {
  if (!ev || typeof ev !== "object") throw new Error("Invalid event: must be an object.");
  if (ev.version !== 1) throw new Error("Invalid event: version must be 1.");
  if (normStr(ev.type) !== "merge") throw new Error("Invalid event: type must be 'merge'.");
  if (!normStr(ev.id)) throw new Error("Invalid event: id is required.");
  if (!normStr(ev.repo_id)) throw new Error("Invalid event: repo_id is required.");
  const prn = ensureInt(ev.pr_number);
  if (prn == null || prn <= 0) throw new Error("Invalid event: pr_number must be a positive integer.");
  if (!normStr(ev.merge_commit_sha)) throw new Error("Invalid event: merge_commit_sha is required.");
  if (!normStr(ev.base_branch)) throw new Error("Invalid event: base_branch is required.");
  if (!Array.isArray(ev.affected_paths)) throw new Error("Invalid event: affected_paths must be an array.");
  if (parseMs(ev.timestamp) == null) throw new Error("Invalid event: timestamp must be an ISO string.");
  return true;
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

async function listSegmentFiles(segmentsDirAbs) {
  if (!existsSync(segmentsDirAbs)) return [];
  const entries = await readdir(segmentsDirAbs, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile() && e.name.endsWith(".jsonl"))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));
}

export async function runLaneAEventsSummary({ projectRoot } = {}) {
  const paths = await loadProjectPaths({ projectRoot: resolve(String(projectRoot || "")) });
  const segmentsDirAbs = paths.laneA.eventsSegmentsAbs;
  const files = await listSegmentFiles(segmentsDirAbs);

  const latestByRepo = new Map();
  const warnings = [];

  for (const f of files) {
    const abs = join(segmentsDirAbs, f);
    // eslint-disable-next-line no-await-in-loop
    const text = await readFile(abs, "utf8");
    const lines = String(text || "")
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);

    for (const line of lines) {
      let obj;
      try {
        obj = JSON.parse(line);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        throw new Error(`Invalid JSONL in ${f}: ${msg}`);
      }
      try {
        validateMergeEventShape(obj);
      } catch (err2) {
        const msg2 = err2 instanceof Error ? err2.message : String(err2);
        warnings.push(`Skipping non-merge event line in ${f}: ${msg2}`);
        continue;
      }
      const repoId = normStr(obj.repo_id);
      const ms = parseMs(obj.timestamp) ?? 0;
      const prev = latestByRepo.get(repoId);
      if (!prev || ms > prev.ms || (ms === prev.ms && String(obj.id).localeCompare(String(prev.event.id)) > 0)) {
        latestByRepo.set(repoId, { ms, event: obj });
      }
    }
  }

  const merge_events = Array.from(latestByRepo.entries())
    .map(([repo_id, v]) => ({
      repo_id,
      latest_merge_commit: normStr(v.event.merge_commit_sha),
      latest_pr_number: ensureInt(v.event.pr_number),
      latest_timestamp: normStr(v.event.timestamp),
    }))
    .sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)));

  const summary = { version: 1, generated_at: nowISO(), merge_events };

  const opsSummaryDirAbs = join(paths.laneA.eventsAbs, "summary");
  const opsSummaryAbs = join(opsSummaryDirAbs, "events-summary.json");
  const knowledgeSummaryAbs = join(paths.knowledge.rootAbs, "events_summary.json");

  await mkdir(opsSummaryDirAbs, { recursive: true });
  await writeTextAtomic(opsSummaryAbs, JSON.stringify(summary, null, 2) + "\n");
  await writeTextAtomic(knowledgeSummaryAbs, JSON.stringify(summary, null, 2) + "\n");

  return { ok: true, projectRoot: paths.opsRootAbs, ops_summary: opsSummaryAbs, knowledge_summary: knowledgeSummaryAbs, summary, warnings };
}

