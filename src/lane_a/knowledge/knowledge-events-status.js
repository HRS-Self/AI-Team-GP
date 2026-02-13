import { existsSync } from "node:fs";
import { readFile, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";

import { loadProjectPaths } from "../../paths/project-paths.js";

async function readJsonOptional(absPath) {
  if (!existsSync(absPath)) return { ok: true, exists: false, json: null };
  try {
    const t = await readFile(absPath, "utf8");
    return { ok: true, exists: true, json: JSON.parse(String(t || "")) };
  } catch (err) {
    return { ok: false, exists: true, json: null, message: err instanceof Error ? err.message : String(err) };
  }
}

export async function runKnowledgeEventsStatus({ projectRoot } = {}) {
  const paths = await loadProjectPaths({ projectRoot: projectRoot ? resolve(String(projectRoot)) : null });
  const eventsRootAbs = join(resolve(paths.laneA.rootAbs), "events");
  const segmentsDir = paths.laneA.eventsSegmentsAbs;
  const checkpointsDir = paths.laneA.eventsCheckpointsAbs;
  const indexAbs = join(eventsRootAbs, "index.json");
  const checkpointAbs = join(checkpointsDir, "last_compacted.json");

  const idxRes = await readJsonOptional(indexAbs);
  if (!idxRes.ok) return { ok: false, message: `Invalid ${indexAbs}: ${idxRes.message}` };

  let segCount = 0;
  try {
    if (existsSync(segmentsDir)) {
      const entries = await readdir(segmentsDir, { withFileTypes: true });
      segCount = entries.filter((e) => e.isFile() && e.name.startsWith("events-") && e.name.endsWith(".jsonl")).length;
    }
  } catch {
    segCount = 0;
  }

  const cpRes = await readJsonOptional(checkpointAbs);
  if (!cpRes.ok) return { ok: false, message: `Invalid ${checkpointAbs}: ${cpRes.message}` };

  const events_total = idxRes.exists && typeof idxRes.json?.events_total === "number" ? idxRes.json.events_total : 0;
  const latest_event = idxRes.exists && typeof idxRes.json?.latest_event_at === "string" ? idxRes.json.latest_event_at : null;

  return {
    ok: true,
    ops_events_root: eventsRootAbs,
    knowledge_summary_path: paths.knowledge.eventsSummaryAbs,
    segments: segCount,
    events_total,
    latest_event,
    unconsumed_since: cpRes.exists ? cpRes.json : null,
  };
}
