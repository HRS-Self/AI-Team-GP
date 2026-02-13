import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, readdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, dirname } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { loadProjectPaths } from "../src/paths/project-paths.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { runKnowledgeCommittee } from "../src/lane_a/knowledge/committee-runner.js";
import { answerDecisionPacket } from "../src/lane_a/knowledge/decision-runner.js";
import { validateDecisionPacket, validateKnowledgeEvent } from "../src/contracts/validators/index.js";
import { runLaneAOrchestrate } from "../src/lane_a/orchestrator-lane-a.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function writeJson(pathAbs, obj) {
  mkdirSync(dirname(resolve(pathAbs)), { recursive: true });
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
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

async function setupOneRepoProject() {
  const root = mkdtempSync(join(tmpdir(), "ai-team-decision-packets-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "t", projectMode: "greenfield", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });
  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });

  const repoA = join(paths.reposRootAbs, "repo-a");
  mkdirSync(join(repoA, "src"), { recursive: true });
  writeFileSync(join(repoA, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoA, "src", "index.js"), "console.log('a')\n", "utf8");
  writeFileSync(join(repoA, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: A\n  version: 1\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoA).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoA).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoA).ok);
  assert.ok(run("git", ["add", "."], repoA).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoA).ok);

  writeJson(join(paths.opsConfigAbs, "REPOS.json"), {
    version: 1,
    repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }],
  });

  writeJson(join(paths.opsConfigAbs, "LLM_PROFILES.json"), {
    version: 1,
    profiles: {
      "committee.repo_architect": { provider: "openai", model: "gpt-5.2-mini" },
      "committee.repo_skeptic": { provider: "openai", model: "gpt-5.2-mini" },
      "committee.integration_chair": { provider: "openai", model: "gpt-5.2-mini" },
    },
  });

  writeKickoffLatestSufficient({ knowledgeRootAbs: paths.knowledge.rootAbs });
  writeJson(join(paths.knowledge.ssotSystemAbs, "minimum.json"), { version: 1, required_facts: [] });

  return { root, opsRootAbs, paths };
}

test("Decision packets are created only when committee next_action == decision_needed (not rescan_needed)", async (t) => {
  const { opsRootAbs, paths } = await setupOneRepoProject();

  const prevRoot = process.env.AI_PROJECT_ROOT;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
    if (typeof prevStub === "string") process.env.AI_TEAM_LLM_STUB = prevStub;
    else delete process.env.AI_TEAM_LLM_STUB;
  });

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

  process.env.AI_TEAM_LLM_STUB = "committee_architect_unknown_evidence";
  const r1 = await runKnowledgeCommittee({ projectRoot: resolve(opsRootAbs), scope: "repo:repo-a", limit: 1, dryRun: false });
  assert.equal(r1.ok, false);
  const decisionsDir = paths.knowledge.decisionsAbs;
  const f1 = readdirSync(decisionsDir, { withFileTypes: true }).filter((e) => e.isFile() && e.name.startsWith("DECISION-") && e.name.endsWith(".json"));
  assert.equal(f1.length, 0);

  process.env.AI_TEAM_LLM_STUB = "committee_architect_no_evidence";
  const r2 = await runKnowledgeCommittee({ projectRoot: resolve(opsRootAbs), scope: "repo:repo-a", limit: 1, dryRun: false });
  assert.equal(r2.ok, false);
  const files = readdirSync(decisionsDir, { withFileTypes: true })
    .filter((e) => e.isFile() && e.name.startsWith("DECISION-") && e.name.endsWith(".json"))
    .map((e) => e.name);
  assert.ok(files.length >= 1);
});

test("decision-answer marks decision answered and emits knowledge_event", async () => {
  const { opsRootAbs, paths } = await setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  try {
    const decisionsDir = paths.knowledge.decisionsAbs;
    mkdirSync(decisionsDir, { recursive: true });
    writeJson(join(decisionsDir, "DECISION-DEC_abc123456789.json"), {
      version: 1,
      decision_id: "DEC_abc123456789",
      scope: "system",
      trigger: "state_machine",
      blocking_state: "COMMITTEE_PENDING",
      context: { summary: "s", why_automation_failed: "w", what_is_known: ["EVID_x"] },
      questions: [
        {
          id: "Q_abc123456789",
          question: "Which option should be chosen?",
          expected_answer_type: "choice",
          constraints: "Choose one: keep|discard|rescan_needed",
          blocks: ["COMMITTEE_PENDING"],
        },
      ],
      assumptions_if_unanswered: "block",
      created_at: "2026-02-08T00:00:00.000Z",
      status: "open",
    });
    const inputAbs = join(paths.opsRootAbs, "answer.txt");
    writeFileSync(inputAbs, "keep\n", "utf8");

    const res = await answerDecisionPacket({ projectRoot: resolve(opsRootAbs), decisionId: "DEC_abc123456789", inputPath: inputAbs, dryRun: false });
    assert.equal(res.ok, true);

    const updated = JSON.parse(readFileSync(join(decisionsDir, "DECISION-DEC_abc123456789.json"), "utf8"));
    validateDecisionPacket(updated);
    assert.equal(updated.status, "answered");

    assert.equal(existsSync(paths.laneA.ledgerAbs), true);
    const line = String(readFileSync(paths.laneA.ledgerAbs, "utf8") || "")
      .trim()
      .split("\n")
      .filter(Boolean)
      .at(-1);
    const ev = JSON.parse(line);
    validateKnowledgeEvent(ev);
    assert.equal(ev.type, "decision_answered");
    assert.equal(ev.decision_id, "DEC_abc123456789");
  } finally {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  }
});

test("Lane A orchestrator blocks on multiple open decisions and resumes only when all are answered", async () => {
  const { opsRootAbs, paths } = await setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  try {
    assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
    assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

    const decisionsDir = paths.knowledge.decisionsAbs;
    mkdirSync(decisionsDir, { recursive: true });
    for (const id of ["DEC_one00000000", "DEC_two00000000"]) {
      writeJson(join(decisionsDir, `DECISION-${id}.json`), {
        version: 1,
        decision_id: id,
        scope: "system",
        trigger: "state_machine",
        blocking_state: "COMMITTEE_PENDING",
        context: { summary: "s", why_automation_failed: "w", what_is_known: ["EVID_x"] },
        questions: [
          {
            id: `Q_${id}`,
            question: "Which option should be chosen?",
            expected_answer_type: "choice",
            constraints: "Choose one: keep|discard|rescan_needed",
            blocks: ["COMMITTEE_PENDING"],
          },
        ],
        assumptions_if_unanswered: "block",
        created_at: "2026-02-08T00:00:00.000Z",
        status: "open",
      });
    }

    const r1 = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: false });
    assert.equal(r1.ok, true);
    const state1 = JSON.parse(readFileSync(join(paths.laneA.checkpointsAbs, "state.json"), "utf8"));
    assert.equal(state1.stage, "DECISION_NEEDED");

    const inputAbs = join(paths.opsRootAbs, "answer.txt");
    writeFileSync(inputAbs, "keep\n", "utf8");
    assert.equal((await answerDecisionPacket({ projectRoot: resolve(opsRootAbs), decisionId: "DEC_one00000000", inputPath: inputAbs, dryRun: false })).ok, true);

    const r2 = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: false });
    assert.equal(r2.ok, true);
    const state2 = JSON.parse(readFileSync(join(paths.laneA.checkpointsAbs, "state.json"), "utf8"));
    assert.equal(state2.stage, "DECISION_NEEDED");

    assert.equal((await answerDecisionPacket({ projectRoot: resolve(opsRootAbs), decisionId: "DEC_two00000000", inputPath: inputAbs, dryRun: false })).ok, true);
    const r3 = await runLaneAOrchestrate({ projectRoot: opsRootAbs, dryRun: false });
    assert.equal(r3.ok, true);
    const state3 = JSON.parse(readFileSync(join(paths.laneA.checkpointsAbs, "state.json"), "utf8"));
    assert.notEqual(state3.stage, "DECISION_NEEDED");
  } finally {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  }
});
