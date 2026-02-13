import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, readdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { runKnowledgeCommittee } from "../src/lane_a/knowledge/committee-runner.js";
import { runWriter } from "../src/writer/writer-runner.js";
import { loadProjectPaths } from "../src/paths/project-paths.js";
import { handleSoftStaleEscalation } from "../src/lane_a/staleness/soft-stale-escalation.js";
import { runKnowledgeUpdateMeeting } from "../src/lane_a/knowledge/knowledge-update-meeting.js";

function run(cmd, args, cwd, env = null) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8", env: env ? { ...process.env, ...env } : process.env });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function writeJson(pathAbs, obj) {
  mkdirSync(dirname(resolve(pathAbs)), { recursive: true });
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function setEnv(t, values) {
  const prev = {};
  for (const [k, v] of Object.entries(values || {})) {
    prev[k] = Object.prototype.hasOwnProperty.call(process.env, k) ? process.env[k] : undefined;
    if (v === null || v === undefined) delete process.env[k];
    else process.env[k] = String(v);
  }
  t.after(() => {
    for (const [k, v] of Object.entries(prev)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  });
}

function setupOneRepoProject() {
  const projectHomeAbs = mkdtempSync(join(tmpdir(), "ai-team-soft-stale-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: projectHomeAbs, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: projectHomeAbs, projectId: "t", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const { opsRootAbs, reposRootAbs, knowledgeRootAbs } = knowledgeRepo;
  const repoId = "repo-a";
  const repoAbs = join(reposRootAbs, repoId);
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: repoId, main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('a')\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

  writeJson(join(opsRootAbs, "config", "REPOS.json"), { version: 1, repos: [{ repo_id: repoId, path: repoId, status: "active", team_id: "Tooling" }] });
  writeJson(join(opsRootAbs, "config", "LLM_PROFILES.json"), {
    version: 1,
    profiles: {
      "committee.repo_architect": { provider: "openai", model: "gpt-5.2-mini" },
      "committee.repo_skeptic": { provider: "openai", model: "gpt-5.2-mini" },
      "committee.integration_chair": { provider: "openai", model: "gpt-5.2-mini" },
      "writer.default": { provider: "openai", model: "gpt-5.2-mini" },
    },
  });
  writeJson(join(opsRootAbs, "config", "POLICIES.json"), { version: 1, policies: [] });
  writeJson(join(opsRootAbs, "config", "AGENTS.json"), {
    version: 3,
    agents: [{ agent_id: "writer-1", enabled: true, role: "writer", implementation: "llm", llm_profile: "writer.default" }],
  });
  writeJson(join(opsRootAbs, "config", "DOCS.json"), {
    version: 1,
    project_key: "t",
    docs_repo_path: knowledgeRootAbs,
    knowledge_repo_path: knowledgeRootAbs,
    max_docs_per_run: 1,
    output_format: "markdown",
    parts_word_target: 500,
    commit: { enabled: false },
  });

  return { projectHomeAbs, opsRootAbs, reposRootAbs, knowledgeRootAbs, repoId, repoAbs };
}

async function makeSoftStaleRepo({ opsRootAbs, repoAbs }) {
  const idx = await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(idx.ok, true);
  const scan = await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(scan.ok, true);
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('a2')\n", "utf8");
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "c2"], repoAbs).ok);
}

function readJsonAbs(pathAbs) {
  return JSON.parse(readFileSync(pathAbs, "utf8"));
}

test("committee prepends soft-stale banner to markdown output", async (t) => {
  const { opsRootAbs, knowledgeRootAbs, repoId, repoAbs } = setupOneRepoProject();
  setEnv(t, {
    AI_PROJECT_ROOT: opsRootAbs,
    AI_TEAM_LLM_STUB: "committee_all_pass",
    LANE_A_SOFT_STALE_BANNER: "true",
  });

  await makeSoftStaleRepo({ opsRootAbs, repoAbs });
  const res = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: `repo:${repoId}`, limit: 1, dryRun: false });
  assert.equal(res.ok, false);

  const archMdAbs = join(knowledgeRootAbs, "ssot", "repos", repoId, "committee", "architect_claims.md");
  const archMd = readFileSync(archMdAbs, "utf8");
  assert.ok(archMd.startsWith("---\n⚠️ SOFT-STALE KNOWLEDGE (DEGRADED OUTPUT)\n"));

  const statusAbs = join(knowledgeRootAbs, "ssot", "repos", repoId, "committee", "committee_status.json");
  const status = readJsonAbs(statusAbs);
  assert.equal(status.degraded, true);
  assert.equal(status.degraded_reason, "soft_stale");
  assert.equal(status.stale, true);
  assert.equal(status.hard_stale, false);
});

test("writer prepends soft-stale banner to run STATUS markdown", async (t) => {
  const { opsRootAbs, repoId, repoAbs } = setupOneRepoProject();
  setEnv(t, {
    AI_PROJECT_ROOT: opsRootAbs,
    AI_TEAM_LLM_STUB: "committee_all_pass",
    LANE_A_SOFT_STALE_BANNER: "true",
  });

  await makeSoftStaleRepo({ opsRootAbs, repoAbs });
  const res = await runWriter({ projectRoot: opsRootAbs, scope: `repo:${repoId}`, docs: "00_Vision", dryRun: false });
  assert.equal(res.ok, true);

  const statusMdAbs = join(opsRootAbs, res.writer_status_md);
  const statusJsonAbs = join(opsRootAbs, res.writer_status_json);
  const statusMd = readFileSync(statusMdAbs, "utf8");
  const statusJson = readJsonAbs(statusJsonAbs);
  assert.ok(statusMd.startsWith("---\n⚠️ SOFT-STALE KNOWLEDGE (DEGRADED OUTPUT)\n"));
  assert.equal(statusJson.degraded, true);
  assert.equal(statusJson.degraded_reason, "soft_stale");
  assert.equal(statusJson.stale, true);
  assert.equal(statusJson.hard_stale, false);
});

test("soft-stale tracker is created and updated across committee/writer runs", async (t) => {
  const { opsRootAbs, repoId, repoAbs } = setupOneRepoProject();
  setEnv(t, {
    AI_PROJECT_ROOT: opsRootAbs,
    AI_TEAM_LLM_STUB: "committee_all_pass",
    LANE_A_SOFT_STALE_BANNER: "true",
  });

  await makeSoftStaleRepo({ opsRootAbs, repoAbs });
  const committeeRes = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: `repo:${repoId}`, limit: 1, dryRun: false });
  assert.equal(committeeRes.ok, false);
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
  const trackerAbs = paths.laneA.softStaleTrackerAbs;
  const tracker1 = readJsonAbs(trackerAbs);
  const firstEntry = tracker1.repos[repoId];
  assert.ok(firstEntry?.first_seen_at);
  assert.ok(firstEntry?.last_seen_at);

  await new Promise((resolveNow) => setTimeout(resolveNow, 15));
  const writerRes = await runWriter({ projectRoot: opsRootAbs, scope: `repo:${repoId}`, docs: "00_Vision", dryRun: false });
  assert.equal(writerRes.ok, true);

  const tracker2 = readJsonAbs(trackerAbs);
  const secondEntry = tracker2.repos[repoId];
  assert.equal(secondEntry.first_seen_at, firstEntry.first_seen_at);
  assert.ok(Date.parse(secondEntry.last_seen_at) >= Date.parse(firstEntry.last_seen_at));
});

test("no escalation before soft-stale threshold", async (t) => {
  const { opsRootAbs, repoId } = setupOneRepoProject();
  setEnv(t, {
    AI_PROJECT_ROOT: opsRootAbs,
    LANE_A_SOFT_STALE_ESCALATE_MODE: "decision_packet",
    LANE_A_SOFT_STALE_ESCALATE_AFTER_MINUTES: "180",
    LANE_A_SOFT_STALE_ESCALATE_CAP_PER_DAY: "3",
  });
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
  const t0 = new Date("2026-02-12T00:00:00.000Z");
  const t1 = new Date("2026-02-12T02:59:00.000Z");
  const snapshot = { scope: `repo:${repoId}`, stale: true, hard_stale: false, reasons: ["head_sha_mismatch"], stale_repos: [repoId], hard_stale_repos: [] };

  const r0 = await handleSoftStaleEscalation({ paths, scope: `repo:${repoId}`, stalenessSnapshot: snapshot, now: t0 });
  assert.equal(r0.ok, true);
  const r1 = await handleSoftStaleEscalation({ paths, scope: `repo:${repoId}`, stalenessSnapshot: snapshot, now: t1 });
  assert.equal(r1.ok, true);
  assert.equal(r1.escalated.length, 0);
  assert.equal(readdirSync(paths.laneA.decisionPacketsAbs).filter((f) => f.startsWith("DP-SOFT-STALE-")).length, 0);
});

test("escalates after threshold and writes exactly one decision packet", async (t) => {
  const { opsRootAbs, repoId } = setupOneRepoProject();
  setEnv(t, {
    AI_PROJECT_ROOT: opsRootAbs,
    LANE_A_SOFT_STALE_ESCALATE_MODE: "decision_packet",
    LANE_A_SOFT_STALE_ESCALATE_AFTER_MINUTES: "180",
    LANE_A_SOFT_STALE_ESCALATE_CAP_PER_DAY: "3",
  });
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
  const t0 = new Date("2026-02-12T00:00:00.000Z");
  const t1 = new Date("2026-02-12T03:01:00.000Z");
  const snapshot = { scope: `repo:${repoId}`, stale: true, hard_stale: false, reasons: ["head_sha_mismatch"], stale_repos: [repoId], hard_stale_repos: [] };

  await handleSoftStaleEscalation({ paths, scope: `repo:${repoId}`, stalenessSnapshot: snapshot, now: t0 });
  const r1 = await handleSoftStaleEscalation({ paths, scope: `repo:${repoId}`, stalenessSnapshot: snapshot, now: t1 });
  assert.equal(r1.ok, true);
  assert.equal(r1.escalated.length, 1);
  assert.equal(r1.escalated[0].mode, "decision_packet");
  assert.equal(r1.escalated[0].created, true);

  const packetAbs = join(opsRootAbs, r1.escalated[0].artifact);
  assert.equal(existsSync(packetAbs), true);
  const packetFiles = readdirSync(paths.laneA.decisionPacketsAbs).filter((f) => f.startsWith("DP-SOFT-STALE-") && f.endsWith(".md"));
  assert.equal(packetFiles.length, 1);
});

test("daily cap prevents additional same-day soft-stale escalations", async (t) => {
  const { opsRootAbs } = setupOneRepoProject();
  setEnv(t, {
    AI_PROJECT_ROOT: opsRootAbs,
    LANE_A_SOFT_STALE_ESCALATE_MODE: "decision_packet",
    LANE_A_SOFT_STALE_ESCALATE_AFTER_MINUTES: "1",
    LANE_A_SOFT_STALE_ESCALATE_CAP_PER_DAY: "1",
  });
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
  const t0 = new Date("2026-02-12T00:00:00.000Z");
  const t1 = new Date("2026-02-12T00:02:00.000Z");
  const t2 = new Date("2026-02-12T00:03:00.000Z");
  const systemSnapshot = { scope: "system", stale: true, hard_stale: false, reasons: ["repo_stale"], stale_repos: ["repo-a", "repo-b"], hard_stale_repos: [] };

  await handleSoftStaleEscalation({ paths, scope: "system", stalenessSnapshot: systemSnapshot, now: t0 });
  const r1 = await handleSoftStaleEscalation({ paths, scope: "system", stalenessSnapshot: systemSnapshot, now: t1 });
  assert.equal(r1.escalated.length, 1);
  const r2 = await handleSoftStaleEscalation({ paths, scope: "system", stalenessSnapshot: systemSnapshot, now: t2 });
  assert.equal(r2.escalated.length, 0);

  const dayStamp = "20260212";
  const counterAbs = join(paths.laneA.stalenessAbs, `soft_stale_escalations_${dayStamp}.json`);
  const counter = readJsonAbs(counterAbs);
  assert.equal(counter.count, 1);
  assert.equal(readdirSync(paths.laneA.decisionPacketsAbs).filter((f) => f.startsWith("DP-SOFT-STALE-")).length, 1);
});

test("does not create a second update meeting when one is already open for scope", async (t) => {
  const { opsRootAbs, repoId } = setupOneRepoProject();
  setEnv(t, {
    AI_PROJECT_ROOT: opsRootAbs,
    LANE_A_SOFT_STALE_ESCALATE_MODE: "update_meeting",
    LANE_A_SOFT_STALE_ESCALATE_AFTER_MINUTES: "1",
    LANE_A_SOFT_STALE_ESCALATE_CAP_PER_DAY: "3",
  });
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
  const started = await runKnowledgeUpdateMeeting({ projectRoot: opsRootAbs, mode: "start", scope: `repo:${repoId}`, dryRun: false });
  assert.equal(started.ok, true);

  const meetingsBefore = readdirSync(paths.laneA.meetingsAbs).filter((n) => n.includes(`__repo-${repoId}`));
  const snapshot = { scope: `repo:${repoId}`, stale: true, hard_stale: false, reasons: ["head_sha_mismatch"], stale_repos: [repoId], hard_stale_repos: [] };
  await handleSoftStaleEscalation({ paths, scope: `repo:${repoId}`, stalenessSnapshot: snapshot, now: new Date("2026-02-12T00:00:00.000Z") });
  const escalated = await handleSoftStaleEscalation({ paths, scope: `repo:${repoId}`, stalenessSnapshot: snapshot, now: new Date("2026-02-12T00:02:00.000Z") });
  assert.equal(escalated.escalated.length, 0);

  const meetingsAfter = readdirSync(paths.laneA.meetingsAbs).filter((n) => n.includes(`__repo-${repoId}`));
  assert.equal(meetingsAfter.length, meetingsBefore.length);
});

test("tracker entry is removed when repo returns to non-stale", async (t) => {
  const { opsRootAbs, repoId } = setupOneRepoProject();
  setEnv(t, {
    AI_PROJECT_ROOT: opsRootAbs,
    LANE_A_SOFT_STALE_ESCALATE_MODE: "decision_packet",
    LANE_A_SOFT_STALE_ESCALATE_AFTER_MINUTES: "180",
    LANE_A_SOFT_STALE_ESCALATE_CAP_PER_DAY: "3",
  });
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
  const staleSnapshot = { scope: `repo:${repoId}`, stale: true, hard_stale: false, reasons: ["head_sha_mismatch"], stale_repos: [repoId], hard_stale_repos: [] };
  const freshSnapshot = { scope: `repo:${repoId}`, stale: false, hard_stale: false, reasons: [], stale_repos: [], hard_stale_repos: [] };

  await handleSoftStaleEscalation({ paths, scope: `repo:${repoId}`, stalenessSnapshot: staleSnapshot, now: new Date("2026-02-12T00:00:00.000Z") });
  const trackerAbs = paths.laneA.softStaleTrackerAbs;
  const t1 = readJsonAbs(trackerAbs);
  assert.ok(t1.repos[repoId]);

  await handleSoftStaleEscalation({ paths, scope: `repo:${repoId}`, stalenessSnapshot: freshSnapshot, now: new Date("2026-02-12T00:10:00.000Z") });
  const t2 = readJsonAbs(trackerAbs);
  assert.equal(Object.prototype.hasOwnProperty.call(t2.repos, repoId), false);
});
