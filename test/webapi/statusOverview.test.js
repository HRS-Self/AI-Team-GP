import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../../src/test-helpers/ssot-fixture.js";
import { registerStatusOverviewRoutes } from "../../src/web/status-overview.js";

function writeJson(absPath, obj) {
  mkdirSync(dirname(absPath), { recursive: true });
  writeFileSync(absPath, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function makeProjectFixture({ root, projectCode }) {
  const projectHomeAbs = join(root, projectCode);
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({
    projectRoot: projectHomeAbs,
    projectId: projectCode,
    activeTeams: ["Tooling"],
    sharedPacks: [],
  });
  writeProjectConfig({
    projectRoot: projectHomeAbs,
    projectId: projectCode,
    knowledgeRepo,
    activeTeams: ["Tooling"],
    sharedPacks: [],
  });

  const repoId = "repo-a";
  mkdirSync(join(knowledgeRepo.reposRootAbs, repoId), { recursive: true });
  writeJson(join(knowledgeRepo.opsRootAbs, "config", "REPOS.json"), {
    version: 1,
    repos: [{ repo_id: repoId, path: repoId, team_id: "Tooling", status: "active" }],
  });

  return {
    project_code: projectCode,
    root_dir: projectHomeAbs,
    ops_dir: knowledgeRepo.opsRootAbs,
    repos_dir: knowledgeRepo.reposRootAbs,
    knowledge_dir: knowledgeRepo.knowledgeRootAbs,
    repo_id: repoId,
  };
}

function toRegistryProject(fixture) {
  const now = new Date().toISOString();
  return {
    project_code: fixture.project_code,
    status: "active",
    root_dir: fixture.root_dir,
    ops_dir: fixture.ops_dir,
    repos_dir: fixture.repos_dir,
    created_at: now,
    updated_at: now,
    ports: { webui_port: 8090, websvc_port: 8091 },
    pm2: {
      ecosystem_path: join(fixture.ops_dir, "pm2", "ecosystem.config.cjs"),
      apps: [`${fixture.project_code}-webui`, `${fixture.project_code}-websvc`],
    },
    cron: { installed: false, entries: [] },
    knowledge: {
      type: "git",
      abs_path: fixture.knowledge_dir,
      git_remote: "",
      default_branch: "main",
      active_branch: "main",
      last_commit_sha: null,
    },
    repos: [
      {
        repo_id: fixture.repo_id,
        owner_repo: `${fixture.project_code}/${fixture.repo_id}`,
        abs_path: join(fixture.repos_dir, fixture.repo_id),
        default_branch: "main",
        active_branch: "main",
        last_seen_head_sha: null,
        active: true,
      },
    ],
  };
}

function writeRegistry({ engineRoot, projects }) {
  const dirAbs = join(engineRoot, "ai", "registry");
  mkdirSync(dirAbs, { recursive: true });
  const now = new Date().toISOString();
  writeJson(join(dirAbs, "REGISTRY.json"), {
    version: 2,
    host_id: "test-host",
    created_at: now,
    updated_at: now,
    ports: { webui_base: 8090, webui_next: 8090, websvc_base: 8091, websvc_next: 8091 },
    projects: projects.map((project) => toRegistryProject(project)),
  });
}

async function startApp({ engineRoot, projectRootHint }) {
  const app = express();
  registerStatusOverviewRoutes(app, { engineRoot, projectRootHint });
  const server = await new Promise((resolvePromise) => {
    const s = app.listen(0, "127.0.0.1", () => resolvePromise(s));
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : null;
  return { server, baseUrl: `http://127.0.0.1:${port}` };
}

function assertOverviewShape(payload) {
  assert.equal(payload.version, 1);
  assert.equal(typeof payload.generated_at, "string");
  assert.equal(typeof payload.laneA, "object");
  assert.equal(typeof payload.laneA.health, "object");
  assert.equal(typeof payload.laneA.health.hard_stale, "boolean");
  assert.equal(typeof payload.laneA.health.stale, "boolean");
  assert.equal(typeof payload.laneA.health.degraded, "boolean");
  assert.equal(payload.laneA.health.last_scan === null || typeof payload.laneA.health.last_scan === "string", true);
  assert.equal(payload.laneA.health.last_merge_event === null || typeof payload.laneA.health.last_merge_event === "string", true);

  assert.equal(typeof payload.laneA.phases, "object");
  for (const phase of ["reverse", "sufficiency", "forward"]) {
    assert.equal(typeof payload.laneA.phases[phase], "object");
    assert.equal(["ok", "pending", "blocked"].includes(payload.laneA.phases[phase].status), true);
    assert.equal(typeof payload.laneA.phases[phase].message, "string");
  }

  assert.equal(Array.isArray(payload.laneA.repos), true);
  for (const repo of payload.laneA.repos) {
    assert.equal(typeof repo.repo_id, "string");
    assert.equal(typeof repo.coverage, "string");
    assert.equal(typeof repo.stale, "boolean");
    assert.equal(typeof repo.hard_stale, "boolean");
    assert.equal(typeof repo.degraded, "boolean");
    assert.equal(typeof repo.committee_status, "object");
    assert.equal(typeof repo.latest_artifacts, "object");
    for (const key of ["refresh_hint", "decision_packet", "update_meeting", "review_meeting", "committee_report", "writer_report"]) {
      const item = repo.latest_artifacts[key];
      assert.equal(item === null || (typeof item.name === "string" && typeof item.url === "string"), true);
      if (item && item.url) assert.equal(item.url.includes("/lane-a/artifact?"), true);
      if (item && item.url) assert.equal(item.url.includes("/opt/"), false);
    }
  }

  assert.equal(typeof payload.laneB, "object");
  assert.equal(Number.isInteger(payload.laneB.inbox_count), true);
  assert.equal(Number.isInteger(payload.laneB.triage_count), true);
  assert.equal(Array.isArray(payload.laneB.active_work), true);
  assert.equal(typeof payload.laneB.watchdog_status, "object");
}

test("status-overview returns valid JSON shape", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-status-overview-shape-"));
  const fixture = makeProjectFixture({ root, projectCode: "alpha" });
  writeRegistry({ engineRoot: root, projects: [fixture] });

  writeJson(join(fixture.knowledge_dir, "ssot", "repos", fixture.repo_id, "scan.json"), {
    version: 1,
    repo_id: fixture.repo_id,
    scanned_at: "2026-02-12T12:00:00.000Z",
    repo_head_sha: "abc123",
    facts: [],
  });
  writeJson(join(fixture.knowledge_dir, "ssot", "repos", fixture.repo_id, "committee", "committee_status.json"), {
    version: 1,
    repo_id: fixture.repo_id,
    evidence_valid: true,
    stale: false,
    hard_stale: false,
    degraded: false,
  });
  writeJson(join(fixture.ops_dir, "ai", "lane_a", "refresh_hints", "RH-20260212_010101000__repo-repo-a.json"), {
    version: 1,
    scope: `repo:${fixture.repo_id}`,
    reason: "stale:repo_stale",
  });
  const mergeSegmentAbs = join(fixture.ops_dir, "ai", "lane_a", "events", "segments", "20260212-120000.jsonl");
  mkdirSync(dirname(mergeSegmentAbs), { recursive: true });
  writeFileSync(mergeSegmentAbs, `${JSON.stringify({ type: "merge", repo_id: fixture.repo_id, timestamp: "2026-02-12T12:10:00.000Z" })}\n`, "utf8");

  mkdirSync(join(fixture.ops_dir, "ai", "lane_b", "inbox"), { recursive: true });
  mkdirSync(join(fixture.ops_dir, "ai", "lane_b", "triage"), { recursive: true });
  mkdirSync(join(fixture.ops_dir, "ai", "lane_b", "work", "W-0001"), { recursive: true });
  writeFileSync(join(fixture.ops_dir, "ai", "lane_b", "inbox", "I-0001.md"), "# intake\n", "utf8");
  writeJson(join(fixture.ops_dir, "ai", "lane_b", "triage", "T-0001.json"), { id: "T-0001" });
  writeJson(join(fixture.ops_dir, "ai", "lane_b", "work", "W-0001", "status.json"), {
    workId: "W-0001",
    stage: "PROPOSED",
    blocked: false,
    updated_at: "20260212_121000000",
  });
  writeFileSync(
    join(fixture.ops_dir, "ai", "lane_b", "ledger.jsonl"),
    `${JSON.stringify({ timestamp: "2026-02-12T12:11:00.000Z", action: "watchdog_started" })}\n`,
    "utf8",
  );

  const { server, baseUrl } = await startApp({ engineRoot: root, projectRootHint: fixture.ops_dir });
  t.after(() => server.close());

  const res = await fetch(`${baseUrl}/api/status-overview`);
  const json = await res.json();
  assert.equal(res.status, 200);
  assertOverviewShape(json);
});

test("status-overview handles missing files gracefully", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-status-overview-missing-"));
  const fixture = makeProjectFixture({ root, projectCode: "alpha" });
  writeRegistry({ engineRoot: root, projects: [fixture] });

  const { server, baseUrl } = await startApp({ engineRoot: root, projectRootHint: fixture.ops_dir });
  t.after(() => server.close());

  const res = await fetch(`${baseUrl}/api/status-overview`);
  const json = await res.json();
  assert.equal(res.status, 200);
  assertOverviewShape(json);
  assert.equal(json.laneB.inbox_count, 0);
  assert.equal(json.laneB.triage_count, 0);
});

test("status-overview computes stale/hard_stale badges from scenario data", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-status-overview-badges-"));
  const fixture = makeProjectFixture({ root, projectCode: "alpha" });
  writeRegistry({ engineRoot: root, projects: [fixture] });

  writeJson(join(fixture.knowledge_dir, "ssot", "repos", fixture.repo_id, "committee", "committee_status.json"), {
    version: 1,
    repo_id: fixture.repo_id,
    evidence_valid: true,
    stale: true,
    hard_stale: true,
    degraded: false,
    staleness: {
      stale: true,
      hard_stale: true,
      reasons: ["merge_event_after_scan"],
    },
  });

  const { server, baseUrl } = await startApp({ engineRoot: root, projectRootHint: fixture.ops_dir });
  t.after(() => server.close());

  const res = await fetch(`${baseUrl}/api/status-overview`);
  const json = await res.json();
  assert.equal(res.status, 200);
  assertOverviewShape(json);
  assert.equal(json.laneA.health.hard_stale, true);
  assert.equal(json.laneA.health.stale, true);
  assert.equal(json.laneA.repos.some((repo) => repo.repo_id === fixture.repo_id && repo.hard_stale === true), true);
});
