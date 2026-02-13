import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runSeedsToIntake } from "../src/pipelines/seeds-to-intake.js";
import { runGapsToIntake } from "../src/pipelines/gaps-to-intake.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { runKnowledgeSufficiencyApprove, runKnowledgeSufficiencyPropose, readSufficiencyRecord } from "../src/lane_a/knowledge/knowledge-sufficiency.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("Lane B exporters are blocked when sufficiency is not sufficient", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-suff-block-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;
  t.after(() => delete process.env.AI_PROJECT_ROOT);

  const projectId = "proj-suff-block";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: [], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: [], sharedPacks: [] });

  // Minimal registries + seeds/gaps so exporters reach the sufficiency gate (staleness stays fresh because no repos are in scope).
  mkdirSync(join(opsRoot, "config"), { recursive: true });
  writeFileSync(join(opsRoot, "config", "TEAMS.json"), JSON.stringify({ version: 1, teams: [{ team_id: "Tooling", name: "Tooling" }] }, null, 2) + "\n", "utf8");
  writeFileSync(join(opsRoot, "config", "REPOS.json"), JSON.stringify({ version: 1, repos: [] }, null, 2) + "\n", "utf8");
  writeFileSync(
    join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "BACKLOG_SEEDS.json"),
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
            target_teams: ["Tooling"],
            target_repos: null,
            acceptance_criteria: ["done"],
            dependencies: { must_run_after: [], can_run_in_parallel_with: [] },
            ssot_refs: [],
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
    join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "GAPS.json"),
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
            target_teams: ["Tooling"],
            target_repos: null,
            acceptance_criteria: ["ok"],
            dependencies: { must_run_after: [], can_run_in_parallel_with: [] },
            ssot_refs: [],
            confidence: 0.5,
          },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  const seeds = await runSeedsToIntake({ phase: 1, limit: 1, dryRun: true });
  assert.equal(seeds.ok, false);
  assert.ok(String(seeds.errors?.[0] || "").includes("Knowledge sufficiency not sufficient"), JSON.stringify(seeds, null, 2));

  const gaps = await runGapsToIntake({ limit: 1, dryRun: true });
  assert.equal(gaps.ok, false);
  assert.ok(String(gaps.errors?.[0] || "").includes("Knowledge sufficiency not sufficient"), JSON.stringify(gaps, null, 2));
});

test("Lane B override logs sufficiency_override event to lane_b ledger", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-suff-override-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;
  process.env.USER = "tester";
  t.after(() => {
    delete process.env.AI_PROJECT_ROOT;
    delete process.env.USER;
  });

  const projectId = "proj-suff-override";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: [], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: [], sharedPacks: [] });

  // Minimal registries + seeds so exporter reaches sufficiency gate.
  mkdirSync(join(opsRoot, "config"), { recursive: true });
  writeFileSync(join(opsRoot, "config", "TEAMS.json"), JSON.stringify({ version: 1, teams: [{ team_id: "Tooling", name: "Tooling" }] }, null, 2) + "\n", "utf8");
  writeFileSync(join(opsRoot, "config", "REPOS.json"), JSON.stringify({ version: 1, repos: [] }, null, 2) + "\n", "utf8");
  writeFileSync(
    join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "BACKLOG_SEEDS.json"),
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
            target_teams: ["Tooling"],
            target_repos: null,
            acceptance_criteria: ["done"],
            dependencies: { must_run_after: [], can_run_in_parallel_with: [] },
            ssot_refs: [],
            confidence: 0.5,
          },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  await runSeedsToIntake({ phase: 1, limit: 1, dryRun: true, forceWithoutSufficiency: true });

  const ledgerAbs = join(opsRoot, "ai", "lane_b", "ledger.jsonl");
  assert.equal(existsSync(ledgerAbs), true);
  const lines = String(readFileSync(ledgerAbs, "utf8") || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  assert.ok(lines.length >= 1);
  let ev = null;
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    try {
      const obj = JSON.parse(lines[i]);
      if (obj && obj.type === "sufficiency_override") {
        ev = obj;
        break;
      }
    } catch {
      // ignore
    }
  }
  assert.ok(ev, "expected sufficiency_override event in lane_b ledger");
  assert.equal(ev.type, "sufficiency_override");
  assert.equal(ev.user, "tester");
});

test("Cannot approve sufficiency when scan coverage is incomplete", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-suff-approve-incomplete-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;
  t.after(() => delete process.env.AI_PROJECT_ROOT);

  const projectId = "proj-suff-approve-incomplete";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  // Active repo exists and is indexable.
  const repoAbs = join(root, "repos", "repo-a");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('a')\n", "utf8");
  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

  // Config points to active repo; run index only (no scan) => incomplete coverage.
  writeFileSync(
    join(opsRoot, "config", "REPOS.json"),
    JSON.stringify({ version: 1, repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }] }, null, 2) + "\n",
    "utf8",
  );
  assert.equal((await runKnowledgeIndex({ projectRoot: opsRoot, dryRun: false })).ok, true);

  await assert.rejects(
    async () => {
      await runKnowledgeSufficiencyApprove({ projectRoot: opsRoot, scope: "system", knowledgeVersion: "v0", by: "Alice", dryRun: true });
    },
    (err) => {
      const msg = String(err?.message || err);
      return msg.includes("scan coverage is incomplete") || msg.includes("hard-stale");
    },
  );
});

test("Sufficiency is versioned: approval for v0 does not satisfy v1", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-suff-versioned-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;
  t.after(() => delete process.env.AI_PROJECT_ROOT);

  const projectId = "proj-suff-versioned";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const repoAbs = join(root, "repos", "repo-a");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('a')\n", "utf8");
  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

  writeFileSync(
    join(opsRoot, "config", "REPOS.json"),
    JSON.stringify({ version: 1, repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }] }, null, 2) + "\n",
    "utf8",
  );
  assert.equal((await runKnowledgeIndex({ projectRoot: opsRoot, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRoot, dryRun: false })).ok, true);

  // Propose + approve v0.
  assert.equal((await runKnowledgeSufficiencyPropose({ projectRoot: opsRoot, scope: "system", knowledgeVersion: "v0", dryRun: false })).ok, true);
  assert.equal((await runKnowledgeSufficiencyApprove({ projectRoot: opsRoot, scope: "system", knowledgeVersion: "v0", by: "Alice", dryRun: false })).ok, true);

  // Advance version pointer to v1.
  const versionAbs = join(opsRoot, "ai", "lane_a", "knowledge_version.json");
  writeFileSync(versionAbs, JSON.stringify({ version: 1, current: "v1", history: [] }, null, 2) + "\n", "utf8");

  const v1 = await readSufficiencyRecord({ projectRoot: opsRoot, scope: "system", knowledgeVersion: "v1" });
  assert.equal(v1.ok, true);
  assert.equal(v1.exists, false);
  assert.equal(v1.sufficiency.status, "insufficient");
});
