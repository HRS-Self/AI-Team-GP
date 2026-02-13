import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { runVersionedKnowledgeUpdateMeeting, runVersionedKnowledgeUpdateAnswer } from "../src/lane_a/knowledge/version-update-meeting.js";
import { readKnowledgeVersionOrDefault } from "../src/lane_a/knowledge/knowledge-version.js";
import { readSufficiencyRecord } from "../src/lane_a/knowledge/knowledge-sufficiency.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function setupOneRepoProject() {
  const projectHomeAbs = mkdtempSync(join(tmpdir(), "ai-team-versioned-update-meeting-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: projectHomeAbs, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: projectHomeAbs, projectId: "t", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const { opsRootAbs, reposRootAbs, knowledgeRootAbs } = knowledgeRepo;

  const repoA = join(reposRootAbs, "repo-a");
  mkdirSync(join(repoA, "src"), { recursive: true });
  writeFileSync(join(repoA, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoA, "src", "index.js"), "console.log('a')\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoA).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoA).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoA).ok);
  assert.ok(run("git", ["add", "."], repoA).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoA).ok);

  writeJson(join(opsRootAbs, "config", "REPOS.json"), {
    version: 1,
    repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }],
  });

  return { projectHomeAbs, opsRootAbs, knowledgeRootAbs, repoA };
}

test("versioned update meeting: one question then answer then approve bumps version + sets sufficiency", async (t) => {
  const { opsRootAbs, knowledgeRootAbs } = setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

  const started = await runVersionedKnowledgeUpdateMeeting({ projectRoot: opsRootAbs, mode: "start", scope: "system", fromVersion: "v1", toVersion: "v1.0.1", dryRun: false });
  assert.equal(started.ok, true);
  assert.ok(String(started.meeting_id || "").startsWith("UM-"));
  assert.ok(String(started.dir || "").includes("/ai/lane_a/meetings/update/"));
  assert.equal(existsSync(join(started.dir, `UPDATE_MEETING-${started.meeting_id}.json`)), true);

  const c1 = await runVersionedKnowledgeUpdateMeeting({ projectRoot: opsRootAbs, mode: "continue", scope: "system", session: started.meeting_id, fromVersion: "v1", toVersion: "v1.0.1", dryRun: false });
  assert.equal(c1.ok, true);
  assert.ok(c1.question && typeof c1.question.question === "string" && c1.question.question.length > 5);

  const closeBlocked = await runVersionedKnowledgeUpdateMeeting({
    projectRoot: opsRootAbs,
    mode: "close",
    scope: "system",
    session: started.meeting_id,
    fromVersion: "v1",
    toVersion: "v1.0.1",
    decision: "approve",
    by: "tester",
    notes: "ok",
    dryRun: false,
  });
  assert.equal(closeBlocked.ok, false);

  const ansPath = join(started.dir, "answer.txt");
  writeFileSync(ansPath, "Answer: yes.\n", "utf8");
  const a1 = await runVersionedKnowledgeUpdateAnswer({ projectRoot: opsRootAbs, session: started.meeting_id, inputPath: ansPath, dryRun: false });
  assert.equal(a1.ok, true);

  const closeOk = await runVersionedKnowledgeUpdateMeeting({
    projectRoot: opsRootAbs,
    mode: "close",
    scope: "system",
    session: started.meeting_id,
    fromVersion: "v1",
    toVersion: "v1.0.1",
    decision: "approve",
    by: "tester",
    notes: "approve",
    dryRun: false,
  });
  assert.equal(closeOk.ok, true);
  assert.equal(closeOk.decision, "approve");

  const kv = await readKnowledgeVersionOrDefault({ projectRoot: opsRootAbs });
  assert.equal(kv.ok, true);
  assert.equal(kv.version.current, "v1.0.1");
  assert.equal(existsSync(join(knowledgeRootAbs, "VERSION.json")), true);

  const suff = await readSufficiencyRecord({ projectRoot: opsRootAbs, scope: "system", knowledgeVersion: "v1.0.1" });
  assert.equal(suff.exists, true);
  assert.equal(String(suff.sufficiency.status), "sufficient");

  const compactRecord = join(knowledgeRootAbs, "decisions", "meetings", "update", `UPDATE_MEETING-${started.meeting_id}.json`);
  assert.equal(existsSync(compactRecord), true);
});

test("versioned update meeting: cannot approve when scan coverage incomplete", async (t) => {
  const { opsRootAbs } = setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  const started = await runVersionedKnowledgeUpdateMeeting({ projectRoot: opsRootAbs, mode: "start", scope: "system", fromVersion: "v1", toVersion: "v1.0.1", dryRun: false });
  assert.equal(started.ok, true);

  // No scan/index, so coverage is incomplete.
  const closeBad = await runVersionedKnowledgeUpdateMeeting({
    projectRoot: opsRootAbs,
    mode: "close",
    scope: "system",
    session: started.meeting_id,
    fromVersion: "v1",
    toVersion: "v1.0.1",
    decision: "approve",
    by: "tester",
    notes: "approve",
    dryRun: false,
  });
  assert.equal(closeBad.ok, false);
  assert.match(String(closeBad.message || ""), /(coverage|stale)/i);
});

test("versioned update meeting: cannot approve when open decision packets exist", async (t) => {
  const { opsRootAbs, knowledgeRootAbs } = setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

  const decisionsDir = join(knowledgeRootAbs, "decisions");
  mkdirSync(decisionsDir, { recursive: true });
  writeJson(join(decisionsDir, "DECISION-open-decision.json"), {
    version: 1,
    decision_id: "open-decision-0001",
    scope: "system",
    trigger: "state_machine",
    blocking_state: "DECISION_NEEDED",
    context: { summary: "Need input", why_automation_failed: "blocked", what_is_known: ["scan complete"] },
    questions: [{ id: "question-0001", question: "Is this OK?", expected_answer_type: "boolean", constraints: "", blocks: ["NEXT"] }],
    assumptions_if_unanswered: "Assume not OK.",
    created_at: new Date().toISOString(),
    status: "open",
  });

  const started = await runVersionedKnowledgeUpdateMeeting({ projectRoot: opsRootAbs, mode: "start", scope: "system", fromVersion: "v1", toVersion: "v1.0.1", dryRun: false });
  assert.equal(started.ok, true);

  const closeBad = await runVersionedKnowledgeUpdateMeeting({
    projectRoot: opsRootAbs,
    mode: "close",
    scope: "system",
    session: started.meeting_id,
    fromVersion: "v1",
    toVersion: "v1.0.1",
    decision: "approve",
    by: "tester",
    notes: "approve",
    dryRun: false,
  });
  assert.equal(closeBad.ok, false);
  assert.match(String(closeBad.message || ""), /open decision/i);
});

test("versioned update meeting: cannot approve when hard-stale (merge event after scan)", async (t) => {
  const { opsRootAbs } = setupOneRepoProject();
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  assert.equal((await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false })).ok, true);

  // Write a merge event after the scan.
  const segmentsDir = join(opsRootAbs, "ai", "lane_a", "events", "segments");
  mkdirSync(segmentsDir, { recursive: true });
  const ts = new Date(Date.now() + 2000);
  const y = String(ts.getUTCFullYear()).padStart(4, "0");
  const mo = String(ts.getUTCMonth() + 1).padStart(2, "0");
  const da = String(ts.getUTCDate()).padStart(2, "0");
  const hh = String(ts.getUTCHours()).padStart(2, "0");
  const mm = String(ts.getUTCMinutes()).padStart(2, "0");
  const ss = String(ts.getUTCSeconds()).padStart(2, "0");
  const segName = `${y}${mo}${da}-${hh}${mm}${ss}.jsonl`;
  const ev = { version: 1, type: "merge", repo_id: "repo-a", timestamp: ts.toISOString() };
  writeFileSync(join(segmentsDir, segName), JSON.stringify(ev) + "\n", "utf8");

  const started = await runVersionedKnowledgeUpdateMeeting({ projectRoot: opsRootAbs, mode: "start", scope: "repo:repo-a", fromVersion: "v1", toVersion: "v1.0.1", dryRun: false });
  assert.equal(started.ok, true);

  const closeBad = await runVersionedKnowledgeUpdateMeeting({
    projectRoot: opsRootAbs,
    mode: "close",
    scope: "repo:repo-a",
    session: started.meeting_id,
    fromVersion: "v1",
    toVersion: "v1.0.1",
    decision: "approve",
    by: "tester",
    notes: "approve",
    dryRun: false,
  });
  assert.equal(closeBad.ok, false);
  assert.match(String(closeBad.message || ""), /hard-stale/i);
});
