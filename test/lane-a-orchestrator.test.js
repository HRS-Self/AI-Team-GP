import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { laneALockPath, loadProjectPaths } from "../src/paths/project-paths.js";
import { runLaneAOrchestrate } from "../src/lane_a/orchestrator-lane-a.js";
import { logMergeEvent } from "../src/lane_b/lane-b-event-logger.js";
import { acquireOpsLock, releaseOpsLock } from "../src/utils/ops-lock.js";

function writeJson(pathAbs, obj) {
  mkdirSync(dirname(resolve(pathAbs)), { recursive: true });
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function minimalRepoIndex(repoId, scannedAtIso = "2026-02-08T00:00:00.000Z") {
  return {
    version: 1,
    repo_id: repoId,
    scanned_at: scannedAtIso,
    head_sha: "a".repeat(40),
    languages: [],
    entrypoints: ["README.md"],
    build_commands: { package_manager: "npm", install: [], lint: [], build: [], test: [], scripts: {}, evidence_files: [] },
    hotspots: [],
    api_surface: { openapi_files: [], routes_controllers: [], events_topics: [] },
    migrations_schema: [],
    cross_repo_dependencies: [],
    fingerprints: { "README.md": { sha256: "a".repeat(64) } },
    dependencies: { version: 1, detected_at: scannedAtIso, mode: "detected", depends_on: [] },
  };
}

function minimalEvidenceRef({ repoId, evidenceId = "EVID_aaaaaaaaaaaa" } = {}) {
  return {
    evidence_id: evidenceId,
    repo_id: repoId,
    file_path: "README.md",
    commit_sha: "a".repeat(40),
    start_line: 1,
    end_line: 1,
    extractor: "test",
    captured_at: "2026-02-08T00:00:00.000Z",
  };
}

function minimalScan(repoId) {
  return {
    repo_id: repoId,
    scanned_at: "20260208_000000000",
    scan_version: 1,
    external_knowledge: [],
    facts: [{ fact_id: "F_x", claim: "Entrypoint: README.md", evidence_ids: ["EVID_aaaaaaaaaaaa"] }],
    unknowns: [],
    contradictions: [],
    coverage: { files_seen: 1, files_indexed: 1 },
  };
}

function writeKickoffLatestSufficient({ knowledgeRootAbs }) {
  const kickoffDir = join(knowledgeRootAbs, "sessions", "kickoff");
  mkdirSync(kickoffDir, { recursive: true });
  writeJson(join(kickoffDir, "LATEST.json"), {
    version: 2,
    updated_at: "2026-02-08T00:00:00.000Z",
    latest_by_scope: {
      system: {
        scope: "system",
        created_at: "2026-02-08T00:00:00.000Z",
        latest_md: "KICKOFF-20260208_000000000__system.md",
        latest_json: "KICKOFF-20260208_000000000__system.json",
        sufficiency: { status: "sufficient", notes: "ok" },
        open_questions_count: 0,
        blocking_questions_count: 0,
      },
    },
  });
}

async function setupProjectHome() {
  const root = mkdtempSync(join(tmpdir(), "ai-team-lane-a-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "t", projectMode: "greenfield", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });
  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
  writeJson(join(paths.opsConfigAbs, "REPOS.json"), { version: 1, repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }] });
  return { root, opsRootAbs, paths };
}

test("Lane A orchestrator chooses index when evidence is none", async () => {
  const { opsRootAbs } = await setupProjectHome();
  const res = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: true });
  assert.equal(res.ok, true);
  assert.equal(res.nextAction.type, "index");
  assert.deepEqual(res.nextAction.target_repos, ["repo-a"]);
});

test("Lane A orchestrator chooses scan when index exists but scan is missing", async () => {
  const { opsRootAbs, paths } = await setupProjectHome();

  const repoIndexDir = join(paths.knowledge.evidenceIndexReposAbs, "repo-a");
  mkdirSync(repoIndexDir, { recursive: true });
  writeJson(join(repoIndexDir, "repo_index.json"), minimalRepoIndex("repo-a"));
  writeJson(join(repoIndexDir, "repo_fingerprints.json"), { repo_id: "repo-a", captured_at: "2026-02-08T00:00:00.000Z", files: [] });

  const res = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: true });
  assert.equal(res.ok, true);
  assert.equal(res.nextAction.type, "scan");
  assert.deepEqual(res.nextAction.target_repos, ["repo-a"]);
});

test("Lane A orchestrator chooses committee when minimum is not sufficient", async () => {
  const { opsRootAbs, paths } = await setupProjectHome();

  writeKickoffLatestSufficient({ knowledgeRootAbs: paths.knowledge.rootAbs });

  const repoIndexDir = join(paths.knowledge.evidenceIndexReposAbs, "repo-a");
  mkdirSync(repoIndexDir, { recursive: true });
  writeJson(join(repoIndexDir, "repo_index.json"), minimalRepoIndex("repo-a"));
  writeJson(join(repoIndexDir, "repo_fingerprints.json"), { repo_id: "repo-a", captured_at: "2026-02-08T00:00:00.000Z", files: [{ path: "README.md", sha256: "a".repeat(64), category: "source" }] });

  const repoSsotDir = join(paths.knowledge.ssotReposAbs, "repo-a");
  mkdirSync(repoSsotDir, { recursive: true });
  writeJson(join(repoSsotDir, "scan.json"), minimalScan("repo-a"));
  const refsDir = join(paths.knowledge.evidenceReposAbs, "repo-a");
  mkdirSync(refsDir, { recursive: true });
  writeFileSync(join(refsDir, "evidence_refs.jsonl"), `${JSON.stringify(minimalEvidenceRef({ repoId: "repo-a" }))}\n`, "utf8");

  writeJson(join(paths.knowledge.ssotSystemAbs, "minimum.json"), { version: 1, required_facts: [{ repo_id: "repo-a", fact_contains: "API contract file" }] });

  const res = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: true });
  assert.equal(res.ok, true);
  assert.equal(res.nextAction.type, "committee");
  assert.equal(res.evidenceState.scan_coverage_complete, true);
});

test("Lane A orchestrator chooses committee when coverage complete and kickoff sufficient but committee outputs are missing", async () => {
  const { opsRootAbs, paths } = await setupProjectHome();

  writeKickoffLatestSufficient({ knowledgeRootAbs: paths.knowledge.rootAbs });

  const repoIndexDir = join(paths.knowledge.evidenceIndexReposAbs, "repo-a");
  mkdirSync(repoIndexDir, { recursive: true });
  writeJson(join(repoIndexDir, "repo_index.json"), minimalRepoIndex("repo-a"));
  writeJson(join(repoIndexDir, "repo_fingerprints.json"), { repo_id: "repo-a", captured_at: "2026-02-08T00:00:00.000Z", files: [{ path: "README.md", sha256: "a".repeat(64), category: "source" }] });

  const repoSsotDir = join(paths.knowledge.ssotReposAbs, "repo-a");
  mkdirSync(repoSsotDir, { recursive: true });
  writeJson(join(repoSsotDir, "scan.json"), minimalScan("repo-a"));
  const refsDir = join(paths.knowledge.evidenceReposAbs, "repo-a");
  mkdirSync(refsDir, { recursive: true });
  writeFileSync(join(refsDir, "evidence_refs.jsonl"), `${JSON.stringify(minimalEvidenceRef({ repoId: "repo-a" }))}\n`, "utf8");

  writeJson(join(paths.knowledge.ssotSystemAbs, "minimum.json"), { version: 1, required_facts: [] });

  const res = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: true });
  assert.equal(res.ok, true);
  assert.equal(res.nextAction.type, "committee");
  assert.deepEqual(res.nextAction.target_repos, ["repo-a"]);
});

test("Lane A orchestrator chooses question when open decisions exist", async () => {
  const { opsRootAbs, paths } = await setupProjectHome();

  mkdirSync(paths.knowledge.decisionsAbs, { recursive: true });
  writeJson(join(paths.knowledge.decisionsAbs, "DECISION-DEC_000000000001.json"), {
    version: 1,
    decision_id: "DEC_000000000001",
    scope: "system",
    trigger: "state_machine",
    blocking_state: "COMMITTEE_PENDING",
    context: { summary: "s", why_automation_failed: "w", what_is_known: ["EVID_aaaaaaaaaaaa"] },
    questions: [
      {
        id: "Q_000000000001",
        question: "Which option should be chosen?",
        expected_answer_type: "choice",
        constraints: "Choose one: a|b",
        blocks: ["READY_FOR_WRITER"],
      },
    ],
    assumptions_if_unanswered: "block",
    created_at: "2026-02-08T00:00:00.000Z",
    status: "open",
  });

  const res = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: true });
  assert.equal(res.ok, true);
  assert.equal(res.nextAction.type, "question");
});

test("Lane A orchestrator marks ready when checkpoint exists and no unresolved decisions", async () => {
  const { opsRootAbs, paths } = await setupProjectHome();

  writeKickoffLatestSufficient({ knowledgeRootAbs: paths.knowledge.rootAbs });
  writeJson(join(paths.knowledge.ssotSystemAbs, "minimum.json"), { version: 1, required_facts: [] });

  const repoIndexDir = join(paths.knowledge.evidenceIndexReposAbs, "repo-a");
  mkdirSync(repoIndexDir, { recursive: true });
  writeJson(join(repoIndexDir, "repo_index.json"), minimalRepoIndex("repo-a"));
  writeJson(join(repoIndexDir, "repo_fingerprints.json"), { repo_id: "repo-a", captured_at: "2026-02-08T00:00:00.000Z", files: [{ path: "README.md", sha256: "a".repeat(64), category: "source" }] });

  const repoSsotDir = join(paths.knowledge.ssotReposAbs, "repo-a");
  mkdirSync(repoSsotDir, { recursive: true });
  writeJson(join(repoSsotDir, "scan.json"), minimalScan("repo-a"));
  const refsDir = join(paths.knowledge.evidenceReposAbs, "repo-a");
  mkdirSync(refsDir, { recursive: true });
  writeFileSync(join(refsDir, "evidence_refs.jsonl"), `${JSON.stringify(minimalEvidenceRef({ repoId: "repo-a" }))}\n`, "utf8");

  const committeeRepoDir = join(paths.knowledge.ssotReposAbs, "repo-a", "committee");
  mkdirSync(committeeRepoDir, { recursive: true });
  writeJson(join(committeeRepoDir, "committee_status.json"), {
    version: 1,
    repo_id: "repo-a",
    evidence_valid: true,
    blocking_issues: [],
    confidence: "high",
    next_action: "proceed",
  });

  const integDir = join(paths.knowledge.ssotSystemAbs, "committee", "integration");
  mkdirSync(integDir, { recursive: true });
  writeJson(join(integDir, "integration_status.json"), { version: 1, evidence_valid: true, integration_gaps: [], decision_needed: false });

  const res = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(res.ok, true);
  assert.equal(res.nextAction.type, "ready");

  const stateAbs = join(paths.laneA.checkpointsAbs, "state.json");
  const mdAbs = join(paths.laneA.checkpointsAbs, "STATE.md");
  assert.equal(existsSync(stateAbs), true);
  assert.equal(existsSync(mdAbs), true);
  const state = JSON.parse(readFileSync(stateAbs, "utf8"));
  assert.equal(state.next_action.type, "ready");
  assert.equal(state.stage, "COMMITTEE_PASSED");
  const md = readFileSync(mdAbs, "utf8");
  assert.ok(md.includes("NEXT ACTION"));
  assert.ok(md.includes("type: ready"));
});

test("Lane A orchestrator skips when lock is already held", async () => {
  const { opsRootAbs } = await setupProjectHome();
  const lockPath = await laneALockPath({ projectRoot: opsRootAbs });
  const lockOwner = {
    pid: process.pid,
    uid: process.getuid?.() ?? null,
    user: process.env.USER || null,
    host: "test-host",
    cwd: process.cwd(),
    command: "node --test",
    project_root: opsRootAbs,
    ai_project_root: opsRootAbs,
  };
  const held = await acquireOpsLock({ lockPath, ttlMs: 60_000, owner: lockOwner });
  assert.equal(held.ok, true);
  assert.equal(held.acquired, true);

  const res = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: true });
  assert.equal(res.ok, true);
  assert.equal(res.skipped, true);
  assert.equal(res.reason, "lock_held");

  const released = await releaseOpsLock({
    lockPath,
    owner: { owner_token: held.lock.owner_token },
  });
  assert.equal(released.ok, true);
  assert.equal(released.released, true);
});

test("lane-a orchestrator does not reference LLM modules", () => {
  const text = readFileSync("src/lane_a/orchestrator-lane-a.js", "utf8");
  assert.equal(new RegExp("createLlmClient|src/llm/").test(text), false);
});

test("Lane A orchestrator chooses kickoff when kickoff is missing and code evidence is low", async () => {
  const { opsRootAbs, paths } = await setupProjectHome();

  writeJson(join(paths.knowledge.ssotSystemAbs, "minimum.json"), { version: 1, required_facts: [] });

  const repoIndexDir = join(paths.knowledge.evidenceIndexReposAbs, "repo-a");
  mkdirSync(repoIndexDir, { recursive: true });
  writeJson(join(repoIndexDir, "repo_index.json"), minimalRepoIndex("repo-a"));
  writeJson(join(repoIndexDir, "repo_fingerprints.json"), { repo_id: "repo-a", captured_at: "2026-02-08T00:00:00.000Z", files: [{ path: "README.md", sha256: "a".repeat(64), category: "config" }] });

  const repoSsotDir = join(paths.knowledge.ssotReposAbs, "repo-a");
  mkdirSync(repoSsotDir, { recursive: true });
  writeJson(join(repoSsotDir, "scan.json"), minimalScan("repo-a"));
  const refsDir = join(paths.knowledge.evidenceReposAbs, "repo-a");
  mkdirSync(refsDir, { recursive: true });
  writeFileSync(join(refsDir, "evidence_refs.jsonl"), `${JSON.stringify(minimalEvidenceRef({ repoId: "repo-a" }))}\n`, "utf8");

  const res = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: true });
  assert.equal(res.ok, true);
  assert.equal(res.nextAction.type, "kickoff");
});

test("Lane A orchestrator writes a refresh hint when staleness is detected and no update meeting is open", async () => {
  const { opsRootAbs, paths } = await setupProjectHome();

  writeJson(join(paths.knowledge.ssotSystemAbs, "minimum.json"), { version: 1, required_facts: [] });

  const repoIndexDir = join(paths.knowledge.evidenceIndexReposAbs, "repo-a");
  mkdirSync(repoIndexDir, { recursive: true });
  writeJson(join(repoIndexDir, "repo_index.json"), minimalRepoIndex("repo-a"));
  writeJson(join(repoIndexDir, "repo_fingerprints.json"), { repo_id: "repo-a", captured_at: "2026-02-08T00:00:00.000Z", files: [{ path: "README.md", sha256: "a".repeat(64), category: "config" }] });

  const repoSsotDir = join(paths.knowledge.ssotReposAbs, "repo-a");
  mkdirSync(repoSsotDir, { recursive: true });
  writeJson(join(repoSsotDir, "scan.json"), minimalScan("repo-a"));
  const refsDir = join(paths.knowledge.evidenceReposAbs, "repo-a");
  mkdirSync(refsDir, { recursive: true });
  writeFileSync(join(refsDir, "evidence_refs.jsonl"), `${JSON.stringify(minimalEvidenceRef({ repoId: "repo-a" }))}\n`, "utf8");

  // Make it stale via merge event after scan (checkpoint indicates it was already seen to avoid triggering refresh).
  mkdirSync(paths.laneA.eventsSegmentsAbs, { recursive: true });
  const segAbs = join(paths.laneA.eventsSegmentsAbs, "events-20260209-00.jsonl");
  const evt = {
    version: 1,
    event_id: "KEVT_repo-a",
    type: "merge",
    scope: "repo:repo-a",
    repo_id: "repo-a",
    work_id: "W-00000001",
    pr_number: null,
    commit: "a".repeat(40),
    artifacts: { paths: [], fingerprints: [] },
    summary: "merge",
    timestamp: "2026-02-09T00:00:00.000Z",
  };
  writeFileSync(segAbs, `${JSON.stringify(evt)}\n`, "utf8");
  mkdirSync(paths.laneA.eventsCheckpointsAbs, { recursive: true });
  writeJson(join(paths.laneA.eventsCheckpointsAbs, "last_refresh.json"), { version: 1, last_processed_event_id: "KEVT_repo-a", last_processed_segment: "20260209-00", updated_at: "2026-02-09T00:00:00.000Z" });

  const res = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(res.ok, true);

  const hintsDirAbs = join(paths.laneA.rootAbs, "refresh_hints");
  assert.equal(existsSync(hintsDirAbs), true);
  const files = readdirSync(hintsDirAbs).filter((f) => f.startsWith("RH-") && f.endsWith(".json"));
  assert.ok(files.length >= 1);
  const hint = JSON.parse(readFileSync(join(hintsDirAbs, files.sort((a, b) => a.localeCompare(b)).at(-1)), "utf8"));
  assert.equal(hint.recommended_action, "knowledge-refresh");
  assert.equal(hint.scope, "repo:repo-a");
});

test("Lane A orchestrator queues Lane B follow-up intake when QA merge event has unmet E2E obligations", async () => {
  const { opsRootAbs, paths } = await setupProjectHome();

  writeKickoffLatestSufficient({ knowledgeRootAbs: paths.knowledge.rootAbs });
  writeJson(join(paths.knowledge.ssotSystemAbs, "minimum.json"), { version: 1, required_facts: [] });

  const repoIndexDir = join(paths.knowledge.evidenceIndexReposAbs, "repo-a");
  mkdirSync(repoIndexDir, { recursive: true });
  writeJson(join(repoIndexDir, "repo_index.json"), minimalRepoIndex("repo-a"));
  writeJson(join(repoIndexDir, "repo_fingerprints.json"), { repo_id: "repo-a", captured_at: "2026-02-08T00:00:00.000Z", files: [{ path: "README.md", sha256: "a".repeat(64), category: "source" }] });

  const repoSsotDir = join(paths.knowledge.ssotReposAbs, "repo-a");
  mkdirSync(repoSsotDir, { recursive: true });
  writeJson(join(repoSsotDir, "scan.json"), minimalScan("repo-a"));
  const refsDir = join(paths.knowledge.evidenceReposAbs, "repo-a");
  mkdirSync(refsDir, { recursive: true });
  writeFileSync(join(refsDir, "evidence_refs.jsonl"), `${JSON.stringify(minimalEvidenceRef({ repoId: "repo-a" }))}\n`, "utf8");

  const committeeRepoDir = join(paths.knowledge.ssotReposAbs, "repo-a", "committee");
  mkdirSync(committeeRepoDir, { recursive: true });
  writeJson(join(committeeRepoDir, "committee_status.json"), {
    version: 1,
    repo_id: "repo-a",
    evidence_valid: true,
    blocking_issues: [],
    confidence: "high",
    next_action: "proceed",
  });

  const integDir = join(paths.knowledge.ssotSystemAbs, "committee", "integration");
  mkdirSync(integDir, { recursive: true });
  writeJson(join(integDir, "integration_status.json"), { version: 1, evidence_valid: true, integration_gaps: [], decision_needed: false });

  await logMergeEvent(
    {
      repo_id: "repo-a",
      pr_number: 99,
      merge_commit_sha: "c".repeat(40),
      base_branch: "main",
      affected_paths: ["src/auth/service.ts"],
      work_id: "W-follow-e2e",
      changed_paths: ["src/auth/service.ts"],
      obligations: {
        must_add_unit: true,
        must_add_integration: true,
        must_add_e2e: true,
      },
      risk_level: "high",
    },
    { projectRoot: opsRootAbs, now: new Date("2026-02-12T00:00:00.000Z"), dryRun: false },
  );

  const res = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(res.ok, true);
  assert.ok(Array.isArray(res.logs));
  assert.ok(res.logs.some((x) => x.executed === "qa_merge_followups"));

  const inboxFiles = readdirSync(paths.laneB.inboxAbs)
    .filter((n) => n.startsWith("I-") && n.endsWith(".md"))
    .sort((a, b) => a.localeCompare(b));
  assert.equal(inboxFiles.length, 1);
  const intakeText = readFileSync(join(paths.laneB.inboxAbs, inboxFiles[0]), "utf8");
  assert.ok(intakeText.includes("Add E2E tests for repo:repo-a."));
  assert.ok(intakeText.includes("Linkage-WorkId: W-follow-e2e"));

  const checkpointAbs = join(paths.laneA.eventsCheckpointsAbs, "consumer-qa-merge-followups.json");
  assert.equal(existsSync(checkpointAbs), true);
});
