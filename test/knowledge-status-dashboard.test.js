import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { runKnowledgeStatus } from "../src/lane_a/knowledge/knowledge-status.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function run(cmd, args, cwd, env = null) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8", env: env ? { ...process.env, ...env } : process.env });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function writeRepoConfig(opsRootAbs, repos) {
  writeFileSync(join(opsRootAbs, "config", "REPOS.json"), JSON.stringify({ version: 1, repos }, null, 2) + "\n", "utf8");
}

function writeApprovedEmptyDependencyGraph(knowledgeRootAbs, { projectCode, repoNodes }) {
  const graphAbs = join(knowledgeRootAbs, "ssot", "system", "dependency_graph.json");
  mkdirSync(join(knowledgeRootAbs, "ssot", "system"), { recursive: true });
  const graph = {
    version: 1,
    generated_at: "2026-02-02T00:00:00.000Z",
    project: { code: String(projectCode) },
    nodes: (Array.isArray(repoNodes) ? repoNodes : []).slice().sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id))),
    edges: [],
    external_projects: [],
  };
  writeFileSync(graphAbs, JSON.stringify(graph, null, 2) + "\n", "utf8");
}

function writeScanArtifacts(knowledgeRootAbs, repoId, { scanned_at, repo_index_scanned_at_iso, head_sha, commit_sha, factEvidenceIds, evidenceRefIds }) {
  const scanAbs = join(knowledgeRootAbs, "ssot", "repos", repoId, "scan.json");
  const refsAbs = join(knowledgeRootAbs, "evidence", "repos", repoId, "evidence_refs.jsonl");
  const idxAbs = join(knowledgeRootAbs, "evidence", "index", "repos", repoId, "repo_index.json");
  mkdirSync(join(knowledgeRootAbs, "ssot", "repos", repoId), { recursive: true });
  mkdirSync(join(knowledgeRootAbs, "evidence", "repos", repoId), { recursive: true });
  mkdirSync(join(knowledgeRootAbs, "evidence", "index", "repos", repoId), { recursive: true });

  const scan = {
    repo_id: repoId,
    scanned_at,
    scan_version: 1,
    external_knowledge: [],
    facts: [
      {
        fact_id: "FACT_1",
        claim: "Entrypoint: src/index.js",
        evidence_ids: factEvidenceIds,
      },
    ],
    unknowns: [],
    contradictions: [],
    coverage: { files_seen: 1, files_indexed: 1 },
  };
  writeFileSync(scanAbs, JSON.stringify(scan, null, 2) + "\n", "utf8");

  const repoIndex = {
    version: 1,
    repo_id: repoId,
    scanned_at: repo_index_scanned_at_iso,
    head_sha,
    languages: ["javascript"],
    entrypoints: ["src/index.js"],
    build_commands: { package_manager: "npm", install: ["npm ci"], lint: [], build: [], test: [], scripts: {}, evidence_files: ["package.json"] },
    api_surface: { openapi_files: [], routes_controllers: [], events_topics: [] },
    migrations_schema: [],
    cross_repo_dependencies: [],
    hotspots: [],
    fingerprints: {},
    dependencies: { version: 1, detected_at: repo_index_scanned_at_iso, mode: "detected", depends_on: [] },
  };
  writeFileSync(idxAbs, JSON.stringify(repoIndex, null, 2) + "\n", "utf8");

  const refs = evidenceRefIds.map((evidence_id) => ({
    evidence_id,
    repo_id: repoId,
    file_path: "src/index.js",
    commit_sha,
    symbol: "main",
    extractor: "test",
    captured_at: "2026-02-10T00:00:00.000Z",
  }));
  const jsonl = refs.map((r) => JSON.stringify(r)).join("\n") + "\n";
  writeFileSync(refsAbs, jsonl, "utf8");
}

function writeLastRefreshCheckpoint(opsRootAbs, { updated_at }) {
  const cpAbs = join(opsRootAbs, "ai", "lane_a", "events", "checkpoints", "last_refresh.json");
  mkdirSync(join(opsRootAbs, "ai", "lane_a", "events", "checkpoints"), { recursive: true });
  writeFileSync(
    cpAbs,
    JSON.stringify({ version: 1, last_processed_event_id: "KEVT_deadbeefdeadbeef", last_processed_segment: "20260210-00", updated_at }, null, 2) + "\n",
    "utf8",
  );
}

test("knowledge-status happy path: complete scan, no orphans, not stale => OK", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-status-ok-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "proj-status-ok", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "proj-status-ok", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const reposRoot = join(root, "repos");
  const repoId = "repo-a";
  const repoAbs = join(reposRoot, repoId);
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('hi')\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs, { GIT_AUTHOR_DATE: "2026-02-01T00:00:00Z", GIT_COMMITTER_DATE: "2026-02-01T00:00:00Z" }).ok);
  const head = run("git", ["rev-parse", "HEAD"], repoAbs);
  assert.ok(head.ok);
  const headSha = head.stdout.trim();

  writeRepoConfig(opsRoot, [{ repo_id: repoId, path: repoId, status: "active", team_id: "Tooling" }]);
  writeApprovedEmptyDependencyGraph(knowledgeRepo.knowledgeRootAbs, { projectCode: "proj-status-ok", repoNodes: [{ repo_id: repoId, team_id: "Tooling", type: "repo" }] });
  writeScanArtifacts(knowledgeRepo.knowledgeRootAbs, repoId, {
    scanned_at: "20260210_000000000",
    repo_index_scanned_at_iso: "2026-02-02T00:00:00.000Z",
    head_sha: headSha,
    commit_sha: headSha,
    factEvidenceIds: ["EVID_00000001"],
    evidenceRefIds: ["EVID_00000001"],
  });
  writeLastRefreshCheckpoint(opsRoot, { updated_at: "2026-02-02T00:00:00.000Z" });

  const st = await runKnowledgeStatus({ projectRoot: opsRoot });
  assert.equal(st.ok, true);
  assert.equal(st.overall, "OK");
  assert.equal(st.repos.length, 1);
  assert.equal(st.repos[0].repo_id, repoId);
  assert.equal(st.repos[0].scan.complete, true);
  assert.equal(st.repos[0].evidence.orphan_claims, 0);
  assert.equal(st.repos[0].freshness.stale, false);

  const statusMdAbs = join(opsRoot, "ai", "lane_a", "STATUS.md");
  assert.equal(existsSync(statusMdAbs), true);
  const md = readFileSync(statusMdAbs, "utf8");
  assert.ok(md.includes("KNOWLEDGE STATUS"));
  assert.ok(md.includes(`- ${repoId}:`));
});

test("knowledge-status stale detection: repo HEAD advances past last_refresh_at => DEGRADED", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-status-stale-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "proj-status-stale", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "proj-status-stale", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const reposRoot = join(root, "repos");
  const repoId = "repo-a";
  const repoAbs = join(reposRoot, repoId);
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('hi')\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "c1"], repoAbs, { GIT_AUTHOR_DATE: "2026-02-01T00:00:00Z", GIT_COMMITTER_DATE: "2026-02-01T00:00:00Z" }).ok);
  const head1 = run("git", ["rev-parse", "HEAD"], repoAbs);
  assert.ok(head1.ok);
  const headSha1 = head1.stdout.trim();

  writeRepoConfig(opsRoot, [{ repo_id: repoId, path: repoId, status: "active", team_id: "Tooling" }]);
  writeApprovedEmptyDependencyGraph(knowledgeRepo.knowledgeRootAbs, { projectCode: "proj-status-stale", repoNodes: [{ repo_id: repoId, team_id: "Tooling", type: "repo" }] });
  writeScanArtifacts(knowledgeRepo.knowledgeRootAbs, repoId, {
    scanned_at: "20260210_000000000",
    repo_index_scanned_at_iso: "2026-02-03T00:00:00.000Z",
    head_sha: headSha1,
    commit_sha: headSha1,
    factEvidenceIds: ["EVID_00000001"],
    evidenceRefIds: ["EVID_00000001"],
  });

  writeLastRefreshCheckpoint(opsRoot, { updated_at: "2026-02-03T00:00:00.000Z" });

  // Advance HEAD after refresh checkpoint.
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('hi2')\n", "utf8");
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "c2"], repoAbs, { GIT_AUTHOR_DATE: "2026-02-05T00:00:00Z", GIT_COMMITTER_DATE: "2026-02-05T00:00:00Z" }).ok);

  const st = await runKnowledgeStatus({ projectRoot: opsRoot });
  assert.equal(st.ok, true);
  assert.equal(st.overall, "DEGRADED");
  assert.equal(st.repos[0].freshness.stale, true);
  assert.equal(st.repos[0].freshness.stale_reason, "head_sha_mismatch");
});

test("knowledge-status orphan_claims triggers DEGRADED (but scan can still be complete)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-status-orphan-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "proj-status-orphan", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "proj-status-orphan", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const reposRoot = join(root, "repos");
  const repoId = "repo-a";
  const repoAbs = join(reposRoot, repoId);
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('hi')\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs, { GIT_AUTHOR_DATE: "2026-02-01T00:00:00Z", GIT_COMMITTER_DATE: "2026-02-01T00:00:00Z" }).ok);
  const head = run("git", ["rev-parse", "HEAD"], repoAbs);
  assert.ok(head.ok);
  const headSha = head.stdout.trim();

  writeRepoConfig(opsRoot, [{ repo_id: repoId, path: repoId, status: "active", team_id: "Tooling" }]);
  writeApprovedEmptyDependencyGraph(knowledgeRepo.knowledgeRootAbs, { projectCode: "proj-status-orphan", repoNodes: [{ repo_id: repoId, team_id: "Tooling", type: "repo" }] });
  writeScanArtifacts(knowledgeRepo.knowledgeRootAbs, repoId, {
    scanned_at: "20260210_000000000",
    repo_index_scanned_at_iso: "2026-02-02T00:00:00.000Z",
    head_sha: headSha,
    commit_sha: headSha,
    factEvidenceIds: ["EVID_MISSING"],
    evidenceRefIds: ["EVID_PRESENT"],
  });
  writeLastRefreshCheckpoint(opsRoot, { updated_at: "2026-02-02T00:00:00.000Z" });

  const st = await runKnowledgeStatus({ projectRoot: opsRoot });
  assert.equal(st.ok, true);
  assert.equal(st.repos[0].scan.complete, true);
  assert.equal(st.repos[0].evidence.orphan_claims, 1);
  assert.equal(st.overall, "DEGRADED");
  assert.deepEqual(st.repos[0].evidence.orphan_samples, ["FACT_1"]);
});
