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
import { runKnowledgeConfirmV1, runKnowledgeKickoffForward, runKnowledgePhaseClose } from "../src/lane_a/phase-runner.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function setupOneRepoProject({ withScan = false, withSufficiency = false } = {}) {
  const projectHomeAbs = mkdtempSync(join(tmpdir(), "ai-team-phases-"));
  const opsRootAbs = join(projectHomeAbs, "ops");
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: projectHomeAbs, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: projectHomeAbs, projectId: "t", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const repoAbs = join(knowledgeRepo.reposRootAbs, "repo-a");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('a')\n", "utf8");
  writeFileSync(join(repoAbs, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: A\n  version: 1\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

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

  // Ensure a deterministic knowledge version for sufficiency matching.
  mkdirSync(join(opsRootAbs, "ai", "lane_a"), { recursive: true });
  writeJson(join(opsRootAbs, "ai", "lane_a", "knowledge_version.json"), { version: 1, current: "v1.0.0", history: [] });

  if (withSufficiency) {
    const suffDir = join(knowledgeRepo.knowledgeRootAbs, "decisions", "sufficiency");
    mkdirSync(suffDir, { recursive: true });
    const rec = {
      version: 1,
      scope: "system",
      knowledge_version: "v1.0.0",
      status: "sufficient",
      decided_by: "tester",
      decided_at: new Date().toISOString(),
      rationale_md_path: null,
      evidence_basis: ["test"],
      blockers: [],
      stale_status: "fresh",
    };
    writeJson(join(suffDir, "LATEST.json"), {
      version: 1,
      updated_at: new Date().toISOString(),
      latest_by_scope: {
        system: {
          scope: "system",
          knowledge_version: "v1.0.0",
          status: "sufficient",
          decided_by: rec.decided_by,
          decided_at: rec.decided_at,
          record_json: "SUFF-TEST.json",
          record: rec,
        },
      },
    });
  }

  return { projectHomeAbs, opsRootAbs, knowledgeRootAbs: knowledgeRepo.knowledgeRootAbs, withScan };
}

async function ensureScanComplete({ opsRootAbs }) {
  const idx = await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(idx.ok, true);
  const scan = await runKnowledgeScan({ projectRoot: opsRootAbs, dryRun: false, forceWithoutDepsApproval: true });
  assert.equal(scan.ok, true);
}

test("forward kickoff blocked when reverse not closed", async (t) => {
  const { opsRootAbs } = setupOneRepoProject({ withScan: true, withSufficiency: true });
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  await ensureScanComplete({ opsRootAbs });
  const c = await runKnowledgeConfirmV1({ projectRoot: opsRootAbs, by: "tester", notes: null, dryRun: false });
  assert.equal(c.ok, true);

  const res = await runKnowledgeKickoffForward({
    projectRoot: opsRootAbs,
    scope: "system",
    start: true,
    cont: false,
    nonInteractive: true,
    sessionText: "{\"inputs\":{\"title\":\"x\"}}",
    maxQuestions: 1,
    dryRun: false,
  });
  assert.equal(res.ok, false);
  assert.deepEqual(res.reasons, ["reverse_not_closed"]);
  assert.equal(typeof res.blocker, "string");
  assert.equal(existsSync(res.blocker), true);
});

test("forward kickoff blocked when scan incomplete", async (t) => {
  const { opsRootAbs } = setupOneRepoProject({ withScan: false, withSufficiency: true });
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  const close = await runKnowledgePhaseClose({ projectRoot: opsRootAbs, phase: "reverse", by: "tester", notes: null, dryRun: false });
  assert.equal(close.ok, true);
  const c = await runKnowledgeConfirmV1({ projectRoot: opsRootAbs, by: "tester", notes: null, dryRun: false });
  assert.equal(c.ok, true);

  const res = await runKnowledgeKickoffForward({
    projectRoot: opsRootAbs,
    scope: "system",
    start: true,
    cont: false,
    nonInteractive: true,
    sessionText: "{\"inputs\":{\"title\":\"x\"}}",
    maxQuestions: 1,
    dryRun: false,
  });
  assert.equal(res.ok, false);
  assert.deepEqual(res.reasons, ["scan_incomplete"]);
});

test("forward kickoff blocked when sufficiency not sufficient", async (t) => {
  const { opsRootAbs } = setupOneRepoProject({ withScan: true, withSufficiency: false });
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  await ensureScanComplete({ opsRootAbs });
  await runKnowledgePhaseClose({ projectRoot: opsRootAbs, phase: "reverse", by: "tester", notes: null, dryRun: false });

  // Manually mark v1 confirmed (confirm-v1 requires sufficiency, which we intentionally omit here).
  const phaseAbs = join(opsRootAbs, "ai", "lane_a", "phases", "PHASE.json");
  const phase = JSON.parse(readFileSync(phaseAbs, "utf8"));
  phase.prereqs.human_confirmed_v1 = true;
  phase.prereqs.human_confirmed_at = new Date().toISOString();
  phase.prereqs.human_confirmed_by = "tester";
  writeJson(phaseAbs, phase);

  const res = await runKnowledgeKickoffForward({
    projectRoot: opsRootAbs,
    scope: "system",
    start: true,
    cont: false,
    nonInteractive: true,
    sessionText: "{\"inputs\":{\"title\":\"x\"}}",
    maxQuestions: 1,
    dryRun: false,
  });
  assert.equal(res.ok, false);
  assert.deepEqual(res.reasons, ["sufficiency_not_sufficient"]);
});

test("forward kickoff blocked when human v1 confirmation missing", async (t) => {
  const { opsRootAbs } = setupOneRepoProject({ withScan: true, withSufficiency: true });
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  await ensureScanComplete({ opsRootAbs });
  await runKnowledgePhaseClose({ projectRoot: opsRootAbs, phase: "reverse", by: "tester", notes: null, dryRun: false });

  const res = await runKnowledgeKickoffForward({
    projectRoot: opsRootAbs,
    scope: "system",
    start: true,
    cont: false,
    nonInteractive: true,
    sessionText: "{\"inputs\":{\"title\":\"x\"}}",
    maxQuestions: 1,
    dryRun: false,
  });
  assert.equal(res.ok, false);
  assert.deepEqual(res.reasons, ["human_confirmed_v1_missing"]);
});

test("confirm-v1 is blocked unless sufficiency is sufficient", async (t) => {
  const { opsRootAbs } = setupOneRepoProject({ withScan: false, withSufficiency: false });
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;
  t.after(() => {
    if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
    else delete process.env.AI_PROJECT_ROOT;
  });

  const res = await runKnowledgeConfirmV1({ projectRoot: opsRootAbs, by: "tester", notes: null, dryRun: false });
  assert.equal(res.ok, false);
  assert.ok(String(res.message || "").includes("prereqs.sufficiency"));
});

test("committee defaults to reverse when forward not started", async (t) => {
  const { opsRootAbs } = setupOneRepoProject({ withScan: true, withSufficiency: true });
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

  await ensureScanComplete({ opsRootAbs });
  const res = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "repo:repo-a", limit: 1, dryRun: false });
  assert.equal(res.ok, true);
  assert.equal(res.phase, "reverse");
});

test("committee runs as forward when forward is in_progress", async (t) => {
  const { opsRootAbs } = setupOneRepoProject({ withScan: true, withSufficiency: true });
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

  await ensureScanComplete({ opsRootAbs });
  await runKnowledgePhaseClose({ projectRoot: opsRootAbs, phase: "reverse", by: "tester", notes: null, dryRun: false });
  const c = await runKnowledgeConfirmV1({ projectRoot: opsRootAbs, by: "tester", notes: null, dryRun: false });
  assert.equal(c.ok, true);

  // Simulate forward started (meeting/kickoff would normally set this).
  const phaseAbs = join(opsRootAbs, "ai", "lane_a", "phases", "PHASE.json");
  const phase = JSON.parse(readFileSync(phaseAbs, "utf8"));
  phase.current_phase = "forward";
  phase.forward.status = "in_progress";
  phase.forward.started_at = new Date().toISOString();
  writeJson(phaseAbs, phase);

  const res = await runKnowledgeCommittee({ projectRoot: opsRootAbs, scope: "repo:repo-a", limit: 1, dryRun: false });
  assert.equal(res.ok, true);
  assert.equal(res.phase, "forward");
});

