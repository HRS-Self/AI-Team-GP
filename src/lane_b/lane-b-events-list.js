import { existsSync } from "node:fs";
import { readFile, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";

import { loadProjectPaths } from "../paths/project-paths.js";

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

async function listSegmentFiles(segmentsDirAbs) {
  if (!existsSync(segmentsDirAbs)) return [];
  const entries = await readdir(segmentsDirAbs, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile() && e.name.endsWith(".jsonl"))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));
}

export async function runLaneBEventsList({ projectRoot, from = null, to = null } = {}) {
  const paths = await loadProjectPaths({ projectRoot: resolve(String(projectRoot || "")) });
  const segmentsDirAbs = paths.laneA.eventsSegmentsAbs;
  const files = await listSegmentFiles(segmentsDirAbs);

  const fromMs = from ? parseMs(from) : null;
  const toMs = to ? parseMs(to) : null;
  if (from != null && fromMs == null) throw new Error("Invalid --from. Expected ISO timestamp.");
  if (to != null && toMs == null) throw new Error("Invalid --to. Expected ISO timestamp.");

  const events = [];
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

      const ms = parseMs(obj.timestamp);
      if (fromMs != null && ms != null && ms < fromMs) continue;
      if (toMs != null && ms != null && ms > toMs) continue;
      events.push(obj);
    }
  }

  events.sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)) || String(a.id).localeCompare(String(b.id)));
  return { ok: true, projectRoot: paths.opsRootAbs, segmentsDirAbs, events, warnings };
}

