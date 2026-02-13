import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { runKnowledgeKickoff } from "../src/lane_a/knowledge/kickoff-runner.js";
import { stableKickoffQuestionId } from "../src/lane_a/knowledge/kickoff-utils.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

test("kickoff stable question ids are deterministic", () => {
  const id1 = stableKickoffQuestionId({ scope: "system", phase: "vision", question: "What is the vision (1–3 sentences)?" });
  const id2 = stableKickoffQuestionId({ scope: "system", phase: "vision", question: "What is the vision (1–3 sentences)?" });
  assert.equal(id1, id2);
  assert.ok(id1.startsWith("KQ_"));
});

test("knowledge-kickoff non-interactive writes kickoff artifacts under knowledge repo and produces open questions when required fields missing", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-kickoff-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  const projectId = "t";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const inputAbs = join(root, "kickoff-input.json");
  writeJson(inputAbs, { inputs: { title: "TMS" } });

  const res = await runKnowledgeKickoff({
    projectRoot: resolve(opsRoot),
    scope: "system",
    start: true,
    cont: false,
    nonInteractive: true,
    inputFile: inputAbs,
    dryRun: false,
  });

  assert.equal(res.ok, true);
  assert.equal(res.scope, "system");
  assert.ok(res.open_questions_count > 0);
  assert.equal(existsSync(join(knowledgeRepo.knowledgeRootAbs, "sessions", "kickoff", "LATEST.json")), true);
  assert.equal(existsSync(join(knowledgeRepo.knowledgeRootAbs, res.latest_files.md)), true);
  assert.equal(existsSync(join(knowledgeRepo.knowledgeRootAbs, res.latest_files.json)), true);
  assert.equal(existsSync(join(knowledgeRepo.knowledgeRootAbs, res.latest_files.history)), true);

  // Safety: no Lane B areas created under project root.
  assert.equal(existsSync(join(opsRoot, "ai", "lane_b", "work")), false);
});

test("knowledge-kickoff --continue with no new info is idempotent (changed=false)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-kickoff-idem-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  const projectId = "t";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const inputAbs = join(root, "kickoff-input.json");
  writeJson(inputAbs, { inputs: { title: "TMS" } });

  const startRes = await runKnowledgeKickoff({
    projectRoot: resolve(opsRoot),
    scope: "system",
    start: true,
    cont: false,
    nonInteractive: true,
    inputFile: inputAbs,
    dryRun: false,
  });
  assert.equal(startRes.ok, true);

  const latestAbs = join(knowledgeRepo.knowledgeRootAbs, "sessions", "kickoff", "LATEST.json");
  const latestBefore = readFileSync(latestAbs, "utf8");

  const contRes = await runKnowledgeKickoff({
    projectRoot: resolve(opsRoot),
    scope: "system",
    start: false,
    cont: true,
    nonInteractive: true,
    inputFile: inputAbs,
    dryRun: false,
  });
  assert.equal(contRes.ok, true);
  assert.equal(contRes.changed, false);

  const latestAfter = readFileSync(latestAbs, "utf8");
  assert.equal(latestAfter, latestBefore);

  // history should not grow when unchanged
  const historyAbs = join(knowledgeRepo.knowledgeRootAbs, "sessions", "kickoff", "kickoff_history.jsonl");
  const lines = readFileSync(historyAbs, "utf8")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  assert.equal(lines.length, 1);
});
