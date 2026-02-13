import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, readdirSync, readFileSync, existsSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { runTriage } from "../src/lane_b/triage-runner.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

test("--triage creates repo-scoped T-*.json + BATCH and marks I-* as processed (non-dry-run)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-triage-ok-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");

  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "triage-ok", activeTeams: ["FrontendApp"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "triage-ok", knowledgeRepo, activeTeams: ["FrontendApp"], sharedPacks: [] });

  mkdirSync(join(root, "ops", "ai", "lane_b", "inbox"), { recursive: true });
  mkdirSync(join(root, "ops", "ai", "lane_b", "inbox", "triaged"), { recursive: true });
  writeFileSync(
    join(root, "ops", "config", "REPOS.json"),
    JSON.stringify(
      {
        version: 1,
        repos: [
          {
            repo_id: "demo-frontend",
            name: "Demo Frontend",
            path: "demo-frontend",
            status: "active",
            team_id: "FrontendApp",
            keywords: ["demo", "frontend"],
            active_branch: "develop",
          },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  mkdirSync(join(root, "repos", "demo-frontend"), { recursive: true });
  writeFileSync(
    join(root, "ops", "ai", "lane_b", "inbox", "I-2026-02-02T00:00:00.000Z-aaaaaa.md"),
    "Intake: Update README of demo-frontend in develop branch\n",
    "utf8",
  );

  const result = await runTriage({ repoRoot: process.cwd(), limit: 10, dryRun: false });
  assert.ok(result.ok, JSON.stringify(result, null, 2));
  assert.equal(result.processed_count, 1);
  assert.equal(result.created_count, 1);

  const triagedFiles = readdirSync(join(root, "ops", "ai", "lane_b", "inbox", "triaged")).sort();
  const triagedItemFile = triagedFiles.find((f) => f.startsWith("T-") && f.endsWith(".json"));
  assert.ok(triagedItemFile, `Expected T-*.json in inbox/triaged, found: ${triagedFiles.join(", ")}`);
  const batchFile = triagedFiles.find((f) => f === "BATCH-I-2026-02-02T00:00:00.000Z-aaaaaa.json");
  assert.ok(batchFile, `Expected BATCH-I-*.json in inbox/triaged, found: ${triagedFiles.join(", ")}`);

  const markerPath = join(root, "ops", "ai", "lane_b", "inbox", ".processed", "I-2026-02-02T00:00:00.000Z-aaaaaa.json");
  assert.ok(existsSync(markerPath), "processed marker should exist");

  const triagedJson = JSON.parse(readFileSync(join(root, "ops", "ai", "lane_b", "inbox", "triaged", triagedItemFile), "utf8"));
  assert.equal(triagedJson.version, 1);
  assert.equal(triagedJson.raw_intake_id, "I-2026-02-02T00:00:00.000Z-aaaaaa");
  assert.equal(triagedJson.repo_id, "demo-frontend");
  assert.equal(triagedJson.team_id, "FrontendApp");
  assert.equal(triagedJson.target_branch, "develop");
  assert.ok(typeof triagedJson.dedupe_key === "string" && triagedJson.dedupe_key.length >= 8);
});

test("bug_report intake is routed to Lane A when knowledge is insufficient/hard-stale (no Lane B triage/work created)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-triage-bug-report-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");

  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "triage-bug", activeTeams: ["FrontendApp"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "triage-bug", knowledgeRepo, activeTeams: ["FrontendApp"], sharedPacks: [] });

  mkdirSync(join(root, "ops", "ai", "lane_b", "inbox"), { recursive: true });
  mkdirSync(join(root, "ops", "ai", "lane_b", "inbox", "triaged"), { recursive: true });
  writeFileSync(
    join(root, "ops", "config", "REPOS.json"),
    JSON.stringify(
      {
        version: 1,
        repos: [
          {
            repo_id: "demo-frontend",
            name: "Demo Frontend",
            path: "demo-frontend",
            status: "active",
            team_id: "FrontendApp",
            keywords: ["demo", "frontend"],
            active_branch: "develop",
          },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  mkdirSync(join(root, "repos", "demo-frontend"), { recursive: true });
  writeFileSync(
    join(root, "ops", "ai", "lane_b", "inbox", "I-2026-02-02T00:00:00.000Z-bug.md"),
    ["Intake: Bug report: login fails in demo-frontend", "Origin: bug_report", "Scope: repo:demo-frontend", ""].join("\n"),
    "utf8",
  );

  const result = await runTriage({ repoRoot: process.cwd(), limit: 10, dryRun: false });
  assert.ok(result.ok, JSON.stringify(result, null, 2));
  assert.equal(result.processed_count, 1);
  assert.equal(result.created_count, 0);

  const markerPath = join(root, "ops", "ai", "lane_b", "inbox", ".processed", "I-2026-02-02T00:00:00.000Z-bug.json");
  assert.ok(existsSync(markerPath), "processed marker should exist for bug_report routed intakes");
  const marker = JSON.parse(readFileSync(markerPath, "utf8"));
  assert.equal(marker.action, "routed_to_lane_a");
  assert.equal(marker.origin, "bug_report");
  assert.equal(marker.scope, "repo:demo-frontend");

  const changeRequestsDir = join(root, "ops", "ai", "lane_a", "change_requests");
  assert.ok(existsSync(changeRequestsDir), "Lane A change_requests dir should exist");
  const crJsonFiles = readdirSync(changeRequestsDir).filter((f) => f.startsWith("CR-") && f.endsWith(".json"));
  assert.ok(crJsonFiles.length >= 1, "should create a Lane A change request record");
});

test("--triage failure does not mark intake as processed", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-triage-fail-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");

  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "triage-fail", activeTeams: ["FrontendApp"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "triage-fail", knowledgeRepo, activeTeams: ["FrontendApp"], sharedPacks: [] });

  mkdirSync(join(root, "ops", "ai", "lane_b", "inbox"), { recursive: true });
  writeFileSync(
    join(root, "ops", "config", "REPOS.json"),
    JSON.stringify(
      {
        version: 1,
        repos: [
          {
            repo_id: "demo-frontend",
            name: "Demo Frontend",
            path: "demo-frontend",
            status: "active",
            team_id: "FrontendApp",
            keywords: ["demo", "frontend"],
            active_branch: "develop",
          },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  mkdirSync(join(root, "repos", "demo-frontend"), { recursive: true });
  writeFileSync(join(root, "ops", "ai", "lane_b", "inbox", "I-2026-02-02T00:00:00.000Z-bbbbbb.md"), "Intake: Something unrelated\n", "utf8");

  const result = await runTriage({ repoRoot: process.cwd(), limit: 10, dryRun: false });
  assert.equal(result.ok, false);

  const markerPath = join(root, "ops", "ai", "lane_b", "inbox", ".processed", "I-2026-02-02T00:00:00.000Z-bbbbbb.json");
  assert.ok(!existsSync(markerPath), "processed marker must not exist on failure");

  const triageDir = join(root, "ops", "ai", "lane_b", "triage");
  const triageFiles = existsSync(triageDir) ? readdirSync(triageDir).filter((f) => f.startsWith("TRIAGE_FAILED-") && f.endsWith(".md")) : [];
  assert.ok(triageFiles.length >= 1, "TRIAGE_FAILED-*.md should exist");
});
