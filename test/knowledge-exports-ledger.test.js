import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, readFileSync, mkdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runSeedsToIntake } from "../src/pipelines/seeds-to-intake.js";
import { runGapsToIntake } from "../src/pipelines/gaps-to-intake.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { spawnSync } from "node:child_process";

function readJsonl(path) {
  if (!existsSync(path)) return [];
  const lines = String(readFileSync(path, "utf8") || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  return lines.map((l) => JSON.parse(l));
}

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("seeds-to-intake and gaps-to-intake append knowledge_exports.jsonl (idempotent)", async () => {
  const projectHomeAbs = mkdtempSync(join(tmpdir(), "ai-team-exports-ledger-"));

  const projectId = "proj-exports";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: projectHomeAbs, projectId, activeTeams: ["FrontendDP"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: projectHomeAbs, projectId, knowledgeRepo, activeTeams: ["FrontendDP"], sharedPacks: [] });
  process.env.AI_PROJECT_ROOT = knowledgeRepo.opsRootAbs;

  // Project registries needed for seed/gap validation.
  writeFileSync(
    join(knowledgeRepo.opsRootAbs, "config", "TEAMS.json"),
    JSON.stringify({ version: 1, teams: [{ team_id: "FrontendDP", name: "FrontendDP" }] }, null, 2) + "\n",
    "utf8",
  );
  writeFileSync(
    join(knowledgeRepo.opsRootAbs, "config", "REPOS.json"),
    JSON.stringify({ version: 1, repos: [{ repo_id: "dp-frontend-portal", path: "DP_Frontend-Portal", status: "active", team_id: "FrontendDP" }] }, null, 2) + "\n",
    "utf8",
  );

  // Make the active repo non-stale by providing a real git repo and running index+scan.
  const repoAbs = join(projectHomeAbs, "repos", "DP_Frontend-Portal");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "dp-frontend-portal", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('dp-frontend-portal')\n", "utf8");
  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);
  assert.equal((await runKnowledgeIndex({ projectRoot: knowledgeRepo.opsRootAbs, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: knowledgeRepo.opsRootAbs, dryRun: false })).ok, true);

  // Minimal BACKLOG_SEEDS.json + GAPS.json under the knowledge repo.
  const knowledgeRootAbs = knowledgeRepo.knowledgeRootAbs;
  writeFileSync(
    join(knowledgeRootAbs, "ssot", "system", "BACKLOG_SEEDS.json"),
    JSON.stringify(
      {
        version: 1,
        project_code: projectId,
        generated_at: new Date().toISOString(),
        items: [
          {
            seed_id: "SEED-001",
            title: "Seed 1",
            summary: "Do seed 1.",
            rationale: "Because.",
            phase: 1,
            priority: "P1",
            target_teams: ["FrontendDP"],
            target_repos: ["dp-frontend-portal"],
            acceptance_criteria: ["done"],
            dependencies: { must_run_after: [], can_run_in_parallel_with: [] },
            ssot_refs: ["ssot/sections/scope.json#x"],
            confidence: 0.5,
          },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );
  writeFileSync(
    join(knowledgeRootAbs, "ssot", "system", "GAPS.json"),
    JSON.stringify(
      {
        version: 1,
        project_code: projectId,
        baseline: "baseline",
        generated_at: new Date().toISOString(),
        items: [
          {
            gap_id: "GAP-001",
            title: "Gap 1",
            summary: "Gap summary",
            observed_evidence: ["e1"],
            impact: "high",
            risk_level: "high",
            recommended_action: "fix it",
            target_teams: ["FrontendDP"],
            target_repos: ["dp-frontend-portal"],
            acceptance_criteria: ["ok"],
            dependencies: { must_run_after: [], can_run_in_parallel_with: [] },
            ssot_refs: ["ssot/sections/constraints.json#y"],
            confidence: 0.5,
          },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  const seedRes = await runSeedsToIntake({ phase: 1, limit: 1, dryRun: false, forceWithoutSufficiency: true });
  assert.equal(seedRes.ok, true, JSON.stringify(seedRes, null, 2));
  const gapRes = await runGapsToIntake({ impact: null, risk: null, limit: 1, dryRun: false, forceWithoutSufficiency: true });
  assert.equal(gapRes.ok, true, JSON.stringify(gapRes, null, 2));

  const ledgerPath = join(knowledgeRepo.opsRootAbs, "ai", "lane_b", "cache", "knowledge_exports.jsonl");
  const lines1 = readJsonl(ledgerPath);
  assert.equal(lines1.length, 2, "expected 2 knowledge export ledger lines");
  assert.ok(lines1.some((l) => l.type === "seed_export" && Array.isArray(l.created_intakes) && l.created_intakes.length === 1));
  assert.ok(lines1.some((l) => l.type === "gap_export" && Array.isArray(l.created_intakes) && l.created_intakes.length === 1));

  // Re-run: should skip promotions and not append new ledger lines.
  const seedRes2 = await runSeedsToIntake({ phase: 1, limit: 1, dryRun: false, forceWithoutSufficiency: true });
  assert.equal(seedRes2.ok, true);
  assert.equal(seedRes2.promoted_count, 0);
  const gapRes2 = await runGapsToIntake({ limit: 1, dryRun: false, forceWithoutSufficiency: true });
  assert.equal(gapRes2.ok, true);
  assert.equal(gapRes2.promoted_count, 0);

  const lines2 = readJsonl(ledgerPath);
  assert.equal(lines2.length, 2, "ledger must remain unchanged on idempotent re-run");
});
