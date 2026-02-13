import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, readdirSync, existsSync, utimesSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { appendEvent, readEventsSince, rotateIfNeeded, compactOlderThan } from "../src/lane_a/knowledge/knowledge-events-store.js";
import { validateKnowledgeChangeEvent } from "../src/contracts/validators/index.js";

function readLines(absPath) {
  return String(readFileSync(absPath, "utf8") || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
}

test("knowledge events store: appendEvent computes stable event_id and writes valid jsonl", async () => {
  const laneAAbs = mkdtempSync(join(tmpdir(), "ai-team-lane-a-events-"));

  const baseEvent = {
    type: "merge",
    scope: "repo:repo-a",
    repo_id: "repo-a",
    work_id: "W-1",
    pr_number: 1,
    commit: "a".repeat(40),
    artifacts: { paths: ["x", "y"], fingerprints: ["bundle:abc"] },
    summary: "s",
    timestamp: "2026-02-09T00:00:00.000Z",
  };

  const r1 = await appendEvent(baseEvent, { opsLaneAAbs: laneAAbs, now: new Date("2026-02-09T00:00:00.000Z"), dryRun: false });
  assert.equal(r1.ok, true);
  validateKnowledgeChangeEvent(r1.event);

  const r2 = await appendEvent(baseEvent, { opsLaneAAbs: laneAAbs, now: new Date("2026-02-09T00:00:00.000Z"), dryRun: false });
  assert.equal(r2.ok, true);
  assert.equal(r2.event.event_id, r1.event.event_id);

  const segAbs = join(r1.ops_events_root, r1.segment);
  const lines = readLines(segAbs);
  assert.equal(lines.length, 2);
  const parsed = lines.map((l) => JSON.parse(l));
  for (const ev of parsed) validateKnowledgeChangeEvent(ev);
});

test("knowledge events store: rotates segment on hour boundary and enforces maxSegmentBytes", async () => {
  const laneAAbs = mkdtempSync(join(tmpdir(), "ai-team-lane-a-events-rotate-"));

  const e = (commit, ts) => ({
    type: "ci_fix",
    scope: "repo:repo-a",
    repo_id: "repo-a",
    work_id: "W-1",
    pr_number: 1,
    commit,
    artifacts: { paths: ["p"], fingerprints: [] },
    summary: "s",
    timestamp: ts,
  });

  const t10 = new Date("2026-02-09T10:00:00.000Z");
  const t11 = new Date("2026-02-09T11:00:00.000Z");
  await appendEvent(e("b".repeat(40), "2026-02-09T10:00:00.000Z"), { opsLaneAAbs: laneAAbs, now: t10, maxSegmentBytes: 10_000, dryRun: false });
  await appendEvent(e("c".repeat(40), "2026-02-09T11:00:00.000Z"), { opsLaneAAbs: laneAAbs, now: t11, maxSegmentBytes: 10_000, dryRun: false });

  const segDir = join(laneAAbs, "events", "segments");
  const files = readdirSync(segDir)
    .filter((f) => String(f).endsWith(".jsonl"))
    .sort((a, b) => String(a).localeCompare(String(b)));
  assert.equal(files.length, 2);
  assert.ok(files[0].includes("20260209-10"));
  assert.ok(files[1].includes("20260209-11"));

  // Force rotate due to size by setting maxSegmentBytes tiny.
  const t12 = new Date("2026-02-09T12:00:00.000Z");
  await rotateIfNeeded(t12, { opsLaneAAbs: laneAAbs, maxSegmentBytes: 1 });
  const idx = JSON.parse(readFileSync(join(laneAAbs, "events", "index.json"), "utf8"));
  assert.ok(String(idx.active_segment).includes("20260209-12"));
});

test("knowledge events store: readEventsSince filters by timestamp and returns ordered events", async () => {
  const laneAAbs = mkdtempSync(join(tmpdir(), "ai-team-lane-a-events-read-"));

  await appendEvent(
    {
      type: "merge",
      scope: "repo:repo-a",
      repo_id: "repo-a",
      work_id: "W-1",
      pr_number: 1,
      commit: "d".repeat(40),
      artifacts: { paths: ["a"], fingerprints: [] },
      summary: "s1",
      timestamp: "2026-02-09T00:00:00.000Z",
    },
    { opsLaneAAbs: laneAAbs, now: new Date("2026-02-09T00:00:00.000Z"), dryRun: false },
  );
  await appendEvent(
    {
      type: "ci_fix",
      scope: "repo:repo-a",
      repo_id: "repo-a",
      work_id: "W-1",
      pr_number: 1,
      commit: "e".repeat(40),
      artifacts: { paths: ["b"], fingerprints: [] },
      summary: "s2",
      timestamp: "2026-02-09T01:00:00.000Z",
    },
    { opsLaneAAbs: laneAAbs, now: new Date("2026-02-09T01:00:00.000Z"), dryRun: false },
  );

  const r = await readEventsSince({ timestamp: "2026-02-09T00:30:00.000Z" }, { opsLaneAAbs: laneAAbs });
  assert.equal(r.ok, true);
  assert.equal(r.events.length, 1);
  assert.equal(r.events[0].type, "ci_fix");
});

test("knowledge events store: compactOlderThan removes old segments and writes checkpoint", async () => {
  const laneAAbs = mkdtempSync(join(tmpdir(), "ai-team-lane-a-events-compact-"));
  const segDir = join(laneAAbs, "events", "segments");
  mkdirSync(segDir, { recursive: true });
  const oldSeg = join(segDir, "events-20250101-00.jsonl");
  writeFileSync(
    oldSeg,
    JSON.stringify({
      version: 1,
      event_id: "KEVT_deadbeefdeadbeef",
      type: "merge",
      scope: "repo:repo-a",
      repo_id: "repo-a",
      work_id: "W-1",
      pr_number: 1,
      commit: "f".repeat(40),
      artifacts: { paths: ["x"], fingerprints: [] },
      summary: "s",
      timestamp: "2025-01-01T00:00:00.000Z",
    }) + "\n",
    "utf8",
  );
  // Ensure old mtime.
  utimesSync(oldSeg, new Date("2025-01-01T00:00:00.000Z"), new Date("2025-01-01T00:00:00.000Z"));

  // Create index pointing to active segment so compactor doesn't touch it.
  mkdirSync(join(laneAAbs, "events"), { recursive: true });
  writeFileSync(
    join(laneAAbs, "events", "index.json"),
    JSON.stringify({ version: 1, updated_at: null, active_segment: "events-20990101-00.jsonl", events_total: 0, latest_event_at: null, segments: [{ file: "events-20250101-00.jsonl", created_at: null, latest_event_at: null, events: 1 }] }, null, 2) + "\n",
    "utf8",
  );

  const res = await compactOlderThan(1, { opsLaneAAbs: laneAAbs });
  assert.equal(res.ok, true);
  assert.equal(res.compacted, 1);
  assert.equal(existsSync(oldSeg), false);
  const cpAbs = join(laneAAbs, "events", "checkpoints", "last_compacted.json");
  assert.ok(existsSync(cpAbs));
});
