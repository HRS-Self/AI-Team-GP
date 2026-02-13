import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { runKnowledgeCommittee } from "../src/lane_a/knowledge/committee-runner.js";
import { runKnowledgeReviewMeeting, runKnowledgeReviewAnswer } from "../src/lane_a/knowledge/knowledge-review-meeting.js";
import { runKnowledgeSufficiencyStatus } from "../src/lane_a/knowledge/knowledge-sufficiency.js";
import { runKnowledgeBundle } from "../src/lane_a/knowledge/knowledge-bundle.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function setupOneRepoProject() {
  const projectHomeAbs = mkdtempSync(join(tmpdir(), "ai-team-review-meeting-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: projectHomeAbs, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: projectHomeAbs, projectId: "t", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const { opsRootAbs, reposRootAbs, knowledgeRootAbs } = knowledgeRepo;

  const repoA = join(reposRootAbs, "repo-a");
  mkdirSync(join(repoA, "src"), { recursive: true });
  writeFileSync(join(repoA, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoA, "src", "index.js"), "console.log('a')\n", "utf8");
  writeFileSync(join(repoA, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: A\n  version: 1\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoA).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoA).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoA).ok);
  assert.ok(run("git", ["add", "."], repoA).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoA).ok);

  writeJson(join(opsRootAbs, "config", "REPOS.json"), {
    version: 1,
    repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }],
  });

  writeJson(join(opsRootAbs, "config", "LLM_PROFILES.json"), {
    version: 1,
    profiles: {
      "committee.repo_architect": { provider: "openai", model: "gpt-5.2-mini" },
      "committee.repo_skeptic": { provider: "openai", model: "gpt-5.2-mini" },
      "committee.integration_chair": { provider: "openai", model: "gpt-5.2-mini" },
    },
  });

  return { projectHomeAbs, opsRootAbs, knowledgeRootAbs, repoA };
}

test("knowledge-review-meeting: creates session folder + MEETING.json and emits one question per run", async (t) => {
  const { opsRootAbs } = setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  process.env.AI_TEAM_LLM_STUB = "committee_all_pass";
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
    if (typeof prevStub === "string") process.env.AI_TEAM_LLM_STUB = prevStub;
    else delete process.env.AI_TEAM_LLM_STUB;
  });

  const idx = await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(idx.ok, true);
  const scan = await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(scan.ok, true);

  assert.equal((await runKnowledgeBundle({ projectRoot: opsRootAbs, scope: "system", dryRun: false })).ok, true);

  // Run committee to completion (repo + integration) so meeting can proceed to questions without invoking LLM here.
  assert.equal((await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "system", limit: 1, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "system", limit: 1, dryRun: false })).ok, true);

  const started = await runKnowledgeReviewMeeting({ projectRoot: opsRootAbs, mode: "start", scope: "system", dryRun: false });
  assert.equal(started.ok, true);
  assert.ok(started.meeting_id.startsWith("M-"));
  assert.ok(started.dir.includes("/ai/lane_a/meetings/"));
  assert.equal(existsSync(join(started.dir, "MEETING.json")), true);
  assert.equal(existsSync(join(started.dir, "MEETING.md")), true);

  const c1 = await runKnowledgeReviewMeeting({ projectRoot: opsRootAbs, mode: "continue", scope: "system", session: started.meeting_id, dryRun: false });
  assert.equal(c1.ok, true);
  assert.equal(c1.status, "waiting_for_answer");
  assert.ok(c1.question && typeof c1.question.question === "string" && c1.question.question.length > 5);

  const qLines1 = String(readFileSync(join(started.dir, "QUESTIONS.jsonl"), "utf8") || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  assert.equal(qLines1.length, 1, "must ask exactly one question per run");

  const c2 = await runKnowledgeReviewMeeting({ projectRoot: opsRootAbs, mode: "continue", scope: "system", session: started.meeting_id, dryRun: false });
  assert.equal(c2.ok, true);
  assert.equal(c2.waiting_for_answer, true);
  const qLines2 = String(readFileSync(join(started.dir, "QUESTIONS.jsonl"), "utf8") || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  assert.equal(qLines2.length, 1, "must not ask a second question while waiting_for_answer");

  const ansPath = join(started.dir, "answer.txt");
  writeFileSync(ansPath, "Vision: ship v1 safely.\n", "utf8");
  const a1 = await runKnowledgeReviewAnswer({ projectRoot: opsRootAbs, session: started.meeting_id, inputPath: ansPath, dryRun: false });
  assert.equal(a1.ok, true);
  assert.equal(a1.status, "open");

  const c3 = await runKnowledgeReviewMeeting({ projectRoot: opsRootAbs, mode: "continue", scope: "system", session: started.meeting_id, dryRun: false });
  assert.equal(c3.ok, true);
  assert.equal(c3.status, "waiting_for_answer");
  const qLines3 = String(readFileSync(join(started.dir, "QUESTIONS.jsonl"), "utf8") || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  assert.equal(qLines3.length, 2, "must ask next question only after an answer is recorded");
});

test("knowledge-review-meeting: close confirm_sufficiency triggers sufficiency propose when conditions met", async (t) => {
  const { opsRootAbs } = setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  process.env.AI_TEAM_LLM_STUB = "committee_all_pass";
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
    if (typeof prevStub === "string") process.env.AI_TEAM_LLM_STUB = prevStub;
    else delete process.env.AI_TEAM_LLM_STUB;
  });

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeBundle({ projectRoot: opsRootAbs, scope: "system", dryRun: false })).ok, true);
  assert.equal((await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "system", limit: 1, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "system", limit: 1, dryRun: false })).ok, true);

  const started = await runKnowledgeReviewMeeting({ projectRoot: opsRootAbs, mode: "start", scope: "system", dryRun: false });
  assert.equal(started.ok, true);

  const closed = await runKnowledgeReviewMeeting({
    projectRoot: opsRootAbs,
    mode: "close",
    scope: "system",
    session: started.meeting_id,
    closeDecision: "confirm_sufficiency",
    closeNotes: "",
    dryRun: false,
  });
  assert.equal(closed.ok, true, JSON.stringify(closed, null, 2));

  const st = await runKnowledgeSufficiencyStatus({ projectRoot: opsRootAbs });
  assert.equal(st.ok, true);
  assert.equal(st.sufficiency.status, "proposed_sufficient");
});

test("knowledge-review-meeting: stale=true blocks confirm_sufficiency close", async (t) => {
  const { opsRootAbs, repoA } = setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  process.env.AI_TEAM_LLM_STUB = "committee_all_pass";
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
    if (typeof prevStub === "string") process.env.AI_TEAM_LLM_STUB = prevStub;
    else delete process.env.AI_TEAM_LLM_STUB;
  });

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeBundle({ projectRoot: opsRootAbs, scope: "system", dryRun: false })).ok, true);
  assert.equal((await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "system", limit: 1, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "system", limit: 1, dryRun: false })).ok, true);

  // Advance repo HEAD after scan to induce staleness.
  writeFileSync(join(repoA, "src", "index.js"), "console.log('a2')\n", "utf8");
  assert.ok(run("git", ["add", "."], repoA).ok);
  assert.ok(run("git", ["commit", "-m", "c2"], repoA).ok);

  const started = await runKnowledgeReviewMeeting({ projectRoot: opsRootAbs, mode: "start", scope: "system", dryRun: false });
  assert.equal(started.ok, true);

  const closed = await runKnowledgeReviewMeeting({
    projectRoot: opsRootAbs,
    mode: "close",
    scope: "system",
    session: started.meeting_id,
    closeDecision: "confirm_sufficiency",
    closeNotes: "",
    dryRun: false,
  });
  assert.equal(closed.ok, false);
  assert.equal(closed.message, "Cannot close with confirm_sufficiency: staleness.stale is true.");
});
