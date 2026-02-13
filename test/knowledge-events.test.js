import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { loadProjectPaths } from "../src/paths/project-paths.js";
import { logMergeEvent } from "../src/lane_b/lane-b-event-logger.js";
import { runLaneBEventsList } from "../src/lane_b/lane-b-events-list.js";
import { runLaneAEventsSummary } from "../src/lane_a/events/lane-a-events-summary.js";
import { readCheckpoint, writeCheckpoint } from "../src/lane_a/events/event-checkpoints.js";

function readJsonl(absPath) {
  const lines = String(readFileSync(absPath, "utf8") || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  return lines.map((l) => JSON.parse(l));
}

test("Lane B merge event logger: writes correct shape and appends to YYYYMMDD-HHMMSS.jsonl segment", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-merge-events-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "p", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "p", projectMode: "greenfield", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });

  const prev = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prev === "string") process.env.AI_PROJECT_ROOT = prev;
    else delete process.env.AI_PROJECT_ROOT;
  });

  const now = new Date("2026-02-10T01:02:03.000Z");
  const r1 = await logMergeEvent(
    { repo_id: "repo-a", pr_number: 12, merge_commit_sha: "a".repeat(40), base_branch: "main", affected_paths: ["b.txt", "a.txt"] },
    { projectRoot: opsRootAbs, now, dryRun: false },
  );
  assert.equal(r1.ok, true);
  assert.ok(/^\d{8}-\d{6}\.jsonl$/.test(r1.segment_file));
  assert.equal(existsSync(r1.segment_path), true);

  const r2 = await logMergeEvent(
    { repo_id: "repo-a", pr_number: 12, merge_commit_sha: "a".repeat(40), base_branch: "main", affected_paths: ["a.txt"] },
    { projectRoot: opsRootAbs, now, dryRun: false },
  );
  assert.equal(r2.ok, true);
  assert.equal(r2.segment_file, r1.segment_file);

  const events = readJsonl(r1.segment_path);
  assert.equal(events.length, 2);
  for (const ev of events) {
    assert.equal(ev.version, 1);
    assert.equal(ev.type, "merge");
    assert.equal(ev.repo_id, "repo-a");
    assert.equal(ev.pr_number, 12);
    assert.equal(ev.merge_commit_sha, "a".repeat(40));
    assert.equal(ev.base_branch, "main");
    assert.ok(Array.isArray(ev.affected_paths));
    assert.ok(typeof ev.timestamp === "string" && ev.timestamp.endsWith("Z"));
    assert.ok(typeof ev.id === "string" && ev.id.startsWith("EV-repo-a-"));
  }

  // Ensure raw jsonl segments are not written into knowledge repo.
  const knowledgeJsonl = readdirSync(paths.knowledge.rootAbs, { withFileTypes: true })
    .filter((e) => e.isFile() && e.name.endsWith(".jsonl"))
    .map((e) => e.name);
  assert.equal(knowledgeJsonl.length, 0);
});

test("lane-b-events-list: filters events by timestamp range", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-merge-events-list-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "p", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "p", projectMode: "greenfield", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const opsRootAbs = join(root, "ops");
  const prev = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prev === "string") process.env.AI_PROJECT_ROOT = prev;
    else delete process.env.AI_PROJECT_ROOT;
  });

  await logMergeEvent({ repo_id: "repo-a", pr_number: 1, merge_commit_sha: "b".repeat(40), base_branch: "main", affected_paths: [] }, { projectRoot: opsRootAbs, now: new Date("2026-02-10T00:00:00.000Z") });
  await logMergeEvent({ repo_id: "repo-a", pr_number: 2, merge_commit_sha: "c".repeat(40), base_branch: "main", affected_paths: [] }, { projectRoot: opsRootAbs, now: new Date("2026-02-10T02:00:00.000Z") });
  await logMergeEvent({ repo_id: "repo-b", pr_number: 3, merge_commit_sha: "d".repeat(40), base_branch: "main", affected_paths: [] }, { projectRoot: opsRootAbs, now: new Date("2026-02-10T03:00:00.000Z") });

  const res = await runLaneBEventsList({ projectRoot: opsRootAbs, from: "2026-02-10T01:00:00.000Z", to: "2026-02-10T03:00:00.000Z" });
  assert.equal(res.ok, true);
  assert.equal(res.events.length, 2);
  assert.equal(res.events[0].repo_id, "repo-a");
  assert.equal(res.events[1].repo_id, "repo-b");
});

test("lane-a-events-summary: writes ops summary + knowledge events_summary.json with latest merge per repo", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-merge-events-summary-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "p", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "p", projectMode: "greenfield", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });

  const prev = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prev === "string") process.env.AI_PROJECT_ROOT = prev;
    else delete process.env.AI_PROJECT_ROOT;
  });

  await logMergeEvent({ repo_id: "repo-a", pr_number: 1, merge_commit_sha: "e".repeat(40), base_branch: "main", affected_paths: [] }, { projectRoot: opsRootAbs, now: new Date("2026-02-10T00:00:00.000Z") });
  await logMergeEvent({ repo_id: "repo-a", pr_number: 2, merge_commit_sha: "f".repeat(40), base_branch: "main", affected_paths: [] }, { projectRoot: opsRootAbs, now: new Date("2026-02-10T01:00:00.000Z") });
  await logMergeEvent({ repo_id: "repo-b", pr_number: 3, merge_commit_sha: "g".repeat(40), base_branch: "main", affected_paths: [] }, { projectRoot: opsRootAbs, now: new Date("2026-02-10T00:30:00.000Z") });

  const res = await runLaneAEventsSummary({ projectRoot: opsRootAbs });
  assert.equal(res.ok, true);
  assert.equal(res.summary.version, 1);
  assert.equal(Array.isArray(res.summary.merge_events), true);
  assert.equal(res.summary.merge_events.length, 2);

  const a = res.summary.merge_events.find((x) => x.repo_id === "repo-a");
  assert.equal(a.latest_pr_number, 2);
  assert.equal(a.latest_merge_commit, "f".repeat(40));

  assert.equal(existsSync(res.ops_summary), true);
  assert.equal(existsSync(res.knowledge_summary), true);
  assert.equal(resolve(res.knowledge_summary), resolve(join(paths.knowledge.rootAbs, "events_summary.json")));

  // Ensure ops raw segments are not created inside knowledge repo.
  assert.equal(existsSync(join(paths.knowledge.rootAbs, "events", "segments")), false);
  assert.equal(existsSync(join(paths.knowledge.rootAbs, "events", "checkpoints")), false);
});

test("event checkpoints: read/write consumer checkpoint contract", async () => {
  const dir = mkdtempSync(join(tmpdir(), "ai-team-events-checkpoints-"));
  const checkpoints = join(dir, "checkpoints");
  mkdirSync(checkpoints, { recursive: true });

  const r1 = await readCheckpoint({ checkpointsDirAbs: checkpoints, consumer: "knowledge-refresh" });
  assert.equal(r1.ok, true);
  assert.equal(r1.exists, false);
  assert.equal(r1.checkpoint.version, 1);
  assert.equal(r1.checkpoint.consumer, "knowledge-refresh");

  const w = await writeCheckpoint({ checkpointsDirAbs: checkpoints, consumer: "knowledge-refresh", last_segment: "20260210-010203", last_offset: 123 });
  assert.equal(w.ok, true);
  assert.equal(w.wrote, true);

  const r2 = await readCheckpoint({ checkpointsDirAbs: checkpoints, consumer: "knowledge-refresh" });
  assert.equal(r2.ok, true);
  assert.equal(r2.exists, true);
  assert.equal(r2.checkpoint.last_read_segment, "20260210-010203");
  assert.equal(r2.checkpoint.last_read_offset, 123);
});

