import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

import { appendEvent } from "../src/lane_a/knowledge/knowledge-events-store.js";
import { loadProjectPaths } from "../src/paths/project-paths.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { runRefreshFromEvents } from "../src/lane_a/knowledge/knowledge-refresh-from-events.js";
import { runLaneAOrchestrate } from "../src/lane_a/orchestrator-lane-a.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function writeJson(abs, obj) {
  mkdirSync(dirname(resolve(abs)), { recursive: true });
  writeFileSync(abs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function gitHead(repoAbs) {
  const r = run("git", ["rev-parse", "HEAD"], repoAbs);
  assert.equal(r.ok, true);
  return r.stdout.trim();
}

function writeKickoffLatestSufficient({ knowledgeRootAbs }) {
  const kickoffDir = join(knowledgeRootAbs, "sessions", "kickoff");
  mkdirSync(kickoffDir, { recursive: true });
  writeJson(join(kickoffDir, "LATEST.json"), {
    version: 2,
    updated_at: "2026-02-09T00:00:00.000Z",
    latest_by_scope: {
      system: {
        scope: "system",
        created_at: "2026-02-09T00:00:00.000Z",
        latest_md: "KICKOFF-20260209_000000000__system.md",
        latest_json: "KICKOFF-20260209_000000000__system.json",
        sufficiency: { status: "sufficient", notes: "ok" },
        open_questions_count: 0,
        blocking_questions_count: 0,
      },
    },
  });
}

test("knowledge-refresh-from-events: processes new events, updates artifacts, and advances checkpoint atomically", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-refresh-events-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "p1", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "p1", projectMode: "greenfield", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });
  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });

  const repoAbs = join(paths.reposRootAbs, "repo-a");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('hi')\n", "utf8");
  writeFileSync(join(repoAbs, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: X\n  version: 1\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

  writeJson(join(paths.opsConfigAbs, "REPOS.json"), { version: 1, repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }] });

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

  const scanAbs = join(paths.knowledge.ssotReposAbs, "repo-a", "scan.json");
  const scanBefore = JSON.parse(readFileSync(scanAbs, "utf8"));

  writeFileSync(join(repoAbs, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: X\n  version: 2\n", "utf8");
  assert.ok(run("git", ["add", "openapi.yaml"], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "bump"], repoAbs).ok);
  const head = gitHead(repoAbs);

  await appendEvent(
    {
      type: "merge",
      scope: "repo:repo-a",
      repo_id: "repo-a",
      work_id: "W-1",
      pr_number: 1,
      commit: head,
      artifacts: { paths: ["openapi.yaml"], fingerprints: [] },
      summary: "merge",
      timestamp: "2026-02-09T02:00:00.000Z",
    },
    { opsLaneAAbs: paths.laneA.rootAbs, now: new Date("2026-02-09T02:00:00.000Z"), dryRun: false },
  );

  const res = await runRefreshFromEvents(opsRootAbs, { dryRun: false, maxEvents: null, stopOnError: true });
  assert.equal(res.ok, true);
  assert.equal(res.report.processed_events, 1);
  assert.ok(Array.isArray(res.report.repos_impacted) && res.report.repos_impacted.includes("repo-a"));

  const scanAfter = JSON.parse(readFileSync(scanAbs, "utf8"));
  assert.notEqual(scanAfter.scan_version, scanBefore.scan_version);

  const cpAbs = join(paths.laneA.eventsCheckpointsAbs, "last_refresh.json");
  assert.equal(existsSync(cpAbs), true);
  const cp = JSON.parse(readFileSync(cpAbs, "utf8"));
  assert.equal(cp.version, 1);
  assert.equal(typeof cp.last_processed_event_id, "string");
  assert.equal(typeof cp.last_processed_segment, "string");

  const res2 = await runRefreshFromEvents(opsRootAbs, { dryRun: false, maxEvents: null, stopOnError: true });
  assert.equal(res2.ok, true);
  assert.equal(res2.report.processed_events, 0);
  const cp2 = JSON.parse(readFileSync(cpAbs, "utf8"));
  assert.deepEqual(cp2, cp);
});

test("knowledge-refresh-from-events: supports partial processing via --max-events and resumes", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-refresh-events-partial-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "p1", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "p1", projectMode: "greenfield", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });
  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });

  const repoAbs = join(paths.reposRootAbs, "repo-a");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('hi')\n", "utf8");
  writeFileSync(join(repoAbs, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: X\n  version: 1\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

  writeJson(join(paths.opsConfigAbs, "REPOS.json"), { version: 1, repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }] });

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

  writeFileSync(join(repoAbs, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: X\n  version: 2\n", "utf8");
  assert.ok(run("git", ["add", "openapi.yaml"], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "bump1"], repoAbs).ok);
  const c1 = gitHead(repoAbs);
  await appendEvent(
    { type: "merge", scope: "repo:repo-a", repo_id: "repo-a", work_id: "W-1", pr_number: 1, commit: c1, artifacts: { paths: ["openapi.yaml"], fingerprints: [] }, summary: "m1", timestamp: "2026-02-09T03:00:00.000Z" },
    { opsLaneAAbs: paths.laneA.rootAbs, now: new Date("2026-02-09T03:00:00.000Z"), dryRun: false },
  );

  writeFileSync(join(repoAbs, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: X\n  version: 3\n", "utf8");
  assert.ok(run("git", ["add", "openapi.yaml"], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "bump2"], repoAbs).ok);
  const c2 = gitHead(repoAbs);
  await appendEvent(
    { type: "merge", scope: "repo:repo-a", repo_id: "repo-a", work_id: "W-1", pr_number: 1, commit: c2, artifacts: { paths: ["openapi.yaml"], fingerprints: [] }, summary: "m2", timestamp: "2026-02-09T04:00:00.000Z" },
    { opsLaneAAbs: paths.laneA.rootAbs, now: new Date("2026-02-09T04:00:00.000Z"), dryRun: false },
  );

  const r1 = await runRefreshFromEvents(opsRootAbs, { dryRun: false, maxEvents: 1, stopOnError: true });
  assert.equal(r1.ok, true);
  assert.equal(r1.report.processed_events, 1);

  const r2 = await runRefreshFromEvents(opsRootAbs, { dryRun: false, maxEvents: null, stopOnError: true });
  assert.equal(r2.ok, true);
  assert.equal(r2.report.processed_events, 1);
});

test("knowledge-refresh-from-events: does not advance checkpoint on error", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-refresh-events-error-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "p1", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "p1", projectMode: "greenfield", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });
  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });

  writeJson(join(paths.opsConfigAbs, "REPOS.json"), { version: 1, repos: [] });
  await appendEvent(
    {
      type: "merge",
      scope: "repo:repo-x",
      repo_id: "repo-x",
      work_id: "W-1",
      pr_number: 1,
      commit: "a".repeat(40),
      artifacts: { paths: ["x"], fingerprints: [] },
      summary: "merge",
      timestamp: "2026-02-09T02:00:00.000Z",
    },
    { opsLaneAAbs: paths.laneA.rootAbs, now: new Date("2026-02-09T02:00:00.000Z"), dryRun: false },
  );

  const cpAbs = join(paths.laneA.eventsCheckpointsAbs, "last_refresh.json");
  assert.equal(existsSync(cpAbs), false);
  const res = await runRefreshFromEvents(opsRootAbs, { dryRun: false, maxEvents: null, stopOnError: true });
  assert.equal(res.ok, false);
  assert.equal(existsSync(cpAbs), false);
});

test("knowledge-refresh-from-events: marks repo committee outputs stale so Lane A orchestrator schedules committee rerun", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-refresh-events-stale-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "p1", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "p1", projectMode: "greenfield", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });
  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });

  writeKickoffLatestSufficient({ knowledgeRootAbs: paths.knowledge.rootAbs });

  const repoAbs = join(paths.reposRootAbs, "repo-a");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('hi')\n", "utf8");
  writeFileSync(join(repoAbs, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: X\n  version: 1\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

  writeJson(join(paths.opsConfigAbs, "REPOS.json"), { version: 1, repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }] });

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

  const committeeDir = join(paths.knowledge.ssotReposAbs, "repo-a", "committee");
  mkdirSync(committeeDir, { recursive: true });
  writeJson(join(committeeDir, "committee_status.json"), {
    version: 1,
    repo_id: "repo-a",
    evidence_valid: true,
    blocking_issues: [],
    confidence: "high",
    next_action: "proceed",
  });

  writeFileSync(join(repoAbs, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: X\n  version: 2\n", "utf8");
  assert.ok(run("git", ["add", "openapi.yaml"], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "bump"], repoAbs).ok);
  const head = gitHead(repoAbs);
  await appendEvent(
    { type: "merge", scope: "repo:repo-a", repo_id: "repo-a", work_id: "W-1", pr_number: 1, commit: head, artifacts: { paths: ["openapi.yaml"], fingerprints: [] }, summary: "merge", timestamp: "2026-02-09T05:00:00.000Z" },
    { opsLaneAAbs: paths.laneA.rootAbs, now: new Date("2026-02-09T05:00:00.000Z"), dryRun: false },
  );

  const ref = await runRefreshFromEvents(opsRootAbs, { dryRun: false, maxEvents: null, stopOnError: true });
  assert.equal(ref.ok, true);
  assert.equal(existsSync(join(committeeDir, "STALE.json")), true);

  const orch = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: true });
  assert.equal(orch.ok, true);
  assert.equal(orch.nextAction.type, "committee");
  assert.deepEqual(orch.nextAction.target_repos, ["repo-a"]);
});
