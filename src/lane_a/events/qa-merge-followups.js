import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import { intakeId } from "../../utils/id.js";
import { formatFsSafeUtcTimestamp } from "../../utils/naming.js";
import { classifyTestEditPath } from "../../lane_b/qa/qa-obligations-audit.js";
import { readCheckpoint, writeCheckpoint } from "./event-checkpoints.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function safeJsonParse(raw) {
  try {
    return { ok: true, json: JSON.parse(String(raw || "")) };
  } catch (err) {
    return { ok: false, message: err instanceof Error ? err.message : String(err) };
  }
}

function parseIsoToTs(isoLike) {
  const s = normStr(isoLike);
  const ms = Date.parse(s);
  if (!s || !Number.isFinite(ms)) return formatFsSafeUtcTimestamp(new Date());
  return formatFsSafeUtcTimestamp(new Date(ms));
}

function shouldCreateE2eFollowup(ev) {
  if (!isPlainObject(ev?.obligations)) return { needed: false, reason: "no_obligations" };
  if (ev.obligations.must_add_e2e !== true) return { needed: false, reason: "no_e2e_obligation" };
  const paths = Array.isArray(ev.changed_paths) ? ev.changed_paths : Array.isArray(ev.affected_paths) ? ev.affected_paths : [];
  const hasE2e = paths.some((p) => classifyTestEditPath(p) === "e2e");
  if (hasE2e) return { needed: false, reason: "e2e_already_merged" };
  return { needed: true, reason: "missing_e2e_changes" };
}

function scopeFromEvent(ev) {
  const repoId = normStr(ev?.repo_id);
  if (repoId) return `repo:${repoId}`;
  return "system";
}

function intakeSeedFromEvent(ev, scope) {
  return [
    normStr(ev?.id),
    normStr(ev?.work_id),
    scope,
    normStr(ev?.merge_sha) || normStr(ev?.merge_commit_sha),
    JSON.stringify(isPlainObject(ev?.obligations) ? ev.obligations : {}),
  ].join("\n");
}

function renderFollowupIntake(ev, scope) {
  const workId = normStr(ev?.work_id) || "unknown";
  const repoId = normStr(ev?.repo_id) || "system";
  const risk = normStr(ev?.risk_level) || "unknown";
  const paths = Array.isArray(ev.changed_paths) ? ev.changed_paths : Array.isArray(ev.affected_paths) ? ev.affected_paths : [];
  const summaryScope = scope === "system" ? "system scope" : scope;
  const lines = [];
  lines.push(`Intake: Add E2E tests for ${summaryScope}.`);
  lines.push("Source: qa_merge_event");
  lines.push("Origin: qa_followup");
  lines.push(`Scope: ${scope}`);
  lines.push(`Linkage-WorkId: ${workId}`);
  lines.push(`Linkage-MergeEventId: ${normStr(ev?.id) || "(missing)"}`);
  lines.push("");
  lines.push("QA follow-up context:");
  lines.push(`- Original workId: ${workId}`);
  lines.push(`- Repo: ${repoId}`);
  lines.push(`- Risk level: ${risk}`);
  lines.push(`- Reason: obligations.must_add_e2e=true but no E2E test file changes were merged.`);
  if (paths.length) {
    lines.push("- Changed paths:");
    for (const p of paths.slice(0, 50)) lines.push(`  - ${String(p)}`);
  }
  lines.push("");
  lines.push("Deliverable:");
  lines.push("- Add or update E2E coverage for the changed behavior.");
  lines.push("- Reference original workId and merged scope in PR notes.");
  lines.push("");
  return lines.join("\n");
}

async function listSegmentFiles(segmentsDirAbs) {
  if (!existsSync(segmentsDirAbs)) return [];
  const entries = await readdir(segmentsDirAbs, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile() && e.name.endsWith(".jsonl"))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));
}

async function writeFollowupMarker({ markerDirAbs, eventId, doc, dryRun }) {
  if (!eventId) return { ok: true, wrote: false };
  const markerAbs = join(markerDirAbs, `${eventId}.json`);
  if (existsSync(markerAbs)) return { ok: true, wrote: false, marker_abs: markerAbs, exists: true };
  if (dryRun) return { ok: true, wrote: false, marker_abs: markerAbs, dry_run: true };
  await mkdir(markerDirAbs, { recursive: true });
  await writeFile(markerAbs, JSON.stringify(doc, null, 2) + "\n", "utf8");
  return { ok: true, wrote: true, marker_abs: markerAbs };
}

export async function runQaMergeFollowups({ paths, dryRun = false, maxEvents = null } = {}) {
  const checkpointsDirAbs = paths?.laneA?.eventsCheckpointsAbs;
  const segmentsDirAbs = paths?.laneA?.eventsSegmentsAbs;
  const markerDirAbs = join(paths?.laneA?.eventsAbs || "", "qa_followups");
  if (!checkpointsDirAbs || !segmentsDirAbs) throw new Error("runQaMergeFollowups: missing lane A events paths.");

  const cpRes = await readCheckpoint({ checkpointsDirAbs, consumer: "qa-merge-followups" });
  const checkpoint = cpRes.checkpoint;
  const files = await listSegmentFiles(segmentsDirAbs);
  const max = typeof maxEvents === "number" && Number.isFinite(maxEvents) ? Math.max(0, Math.floor(maxEvents)) : null;

  const created = [];
  const skipped = [];
  const warnings = [];
  let processedLines = 0;
  let seenEvents = 0;

  let cursorSeg = checkpoint.last_read_segment;
  let cursorOffset = Number.isFinite(Number(checkpoint.last_read_offset)) ? Number(checkpoint.last_read_offset) : 0;

  for (const fileName of files) {
    const fileAbs = join(segmentsDirAbs, fileName);
    const inAnchorSegment = checkpoint.last_read_segment && fileName === checkpoint.last_read_segment;
    const afterAnchor = !checkpoint.last_read_segment || fileName.localeCompare(checkpoint.last_read_segment) > 0 || inAnchorSegment;
    if (!afterAnchor) continue;

    // eslint-disable-next-line no-await-in-loop
    const text = await readFile(fileAbs, "utf8");
    const rawLines = String(text || "").split("\n");
    for (let i = 0; i < rawLines.length; i += 1) {
      const line = String(rawLines[i] || "").trim();
      if (!line) continue;
      if (inAnchorSegment && i <= checkpoint.last_read_offset) continue;

      processedLines += 1;
      cursorSeg = fileName;
      cursorOffset = i;

      const parsed = safeJsonParse(line);
      if (!parsed.ok) {
        warnings.push(`Invalid JSON in ${fileName}:${i + 1} (${parsed.message})`);
        continue;
      }
      const ev = parsed.json;
      if (!isPlainObject(ev) || normStr(ev.type) !== "merge") continue;
      if (max != null && seenEvents >= max) break;
      seenEvents += 1;

      const qaEval = shouldCreateE2eFollowup(ev);
      if (!qaEval.needed) {
        skipped.push({ event_id: normStr(ev.id) || null, reason: qaEval.reason });
        continue;
      }

      const scope = scopeFromEvent(ev);
      const eventId = normStr(ev.id);
      const seed = intakeSeedFromEvent(ev, scope);
      const ts = parseIsoToTs(ev.timestamp);
      const intakeFileId = intakeId({ timestamp: ts, text: seed });
      const intakeRel = `ai/lane_b/inbox/${intakeFileId}.md`;
      const intakeAbs = resolve(paths.opsRootAbs, intakeRel);
      const markerDoc = {
        version: 1,
        event_id: eventId || null,
        work_id: normStr(ev.work_id) || null,
        scope,
        followup_type: "ADD_E2E_TESTS",
        intake_path: intakeRel,
        created_at: new Date().toISOString(),
      };
      // eslint-disable-next-line no-await-in-loop
      const marker = await writeFollowupMarker({ markerDirAbs, eventId, doc: markerDoc, dryRun });
      if (marker.exists) {
        skipped.push({ event_id: eventId || null, reason: "already_processed", marker: marker.marker_abs });
        continue;
      }

      if (!dryRun && !existsSync(intakeAbs)) {
        const intakeText = renderFollowupIntake(ev, scope);
        // eslint-disable-next-line no-await-in-loop
        await mkdir(paths.laneB.inboxAbs, { recursive: true });
        // eslint-disable-next-line no-await-in-loop
        await writeFile(intakeAbs, intakeText, "utf8");
      }

      created.push({
        event_id: eventId || null,
        work_id: normStr(ev.work_id) || null,
        scope,
        intake_path: intakeRel,
        reason: qaEval.reason,
      });
    }
    if (max != null && seenEvents >= max) break;
  }

  if (!dryRun && cursorSeg) {
    await writeCheckpoint({
      checkpointsDirAbs,
      consumer: "qa-merge-followups",
      last_segment: cursorSeg,
      last_offset: cursorOffset,
      dryRun: false,
    });
  }

  return {
    ok: true,
    dry_run: !!dryRun,
    processed_lines: processedLines,
    merge_events_seen: seenEvents,
    created_count: created.length,
    created,
    skipped_count: skipped.length,
    skipped,
    warnings,
  };
}
