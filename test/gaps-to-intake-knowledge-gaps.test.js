import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runGapsToIntake } from "../src/pipelines/gaps-to-intake.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("--gaps-to-intake can consume knowledge/system/gaps.json", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-gaps-to-intake-knowledge-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  mkdirSync(join(opsRoot, "ai", "lane_b"), { recursive: true });
  mkdirSync(join(opsRoot, "config"), { recursive: true });

  const projectId = "proj-gap-export";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  writeFileSync(join(opsRoot, "config", "TEAMS.json"), JSON.stringify({ version: 1, teams: [{ team_id: "Tooling", description: "t", scope_hints: [], risk_level: "normal" }] }, null, 2) + "\n", "utf8");
  writeFileSync(
    join(opsRoot, "config", "REPOS.json"),
    JSON.stringify({ version: 1, repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }] }, null, 2) + "\n",
    "utf8",
  );

  // Make the active repo non-stale by providing a real git repo and running index+scan.
  const repoAbs = join(root, "repos", "repo-a");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('repo-a')\n", "utf8");
  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);
  assert.equal((await runKnowledgeIndex({ projectRoot: opsRoot, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRoot, dryRun: false })).ok, true);

  const gapsPath = join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "gaps.json");
  mkdirSync(join(knowledgeRepo.knowledgeRootAbs, "ssot", "system"), { recursive: true });
  writeFileSync(
    gapsPath,
    JSON.stringify(
      {
        version: 1,
        scope: "system",
        captured_at: new Date().toISOString(),
        extractor_version: "1",
        gaps: [
          {
            scope: "system",
            category: "integration_missing",
            severity: "high",
            risk: "high",
            summary: "Missing provider for endpoint GET /api/x",
            expected: "Provider exists",
            observed: "Not found",
            evidence: [{ type: "endpoint", method: "GET", path: "/api/x" }],
            suggested_intake: { repo_id: "repo-a", title: "Fix integration", body: "Add endpoint", labels: ["gap", "ai"] },
          },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  const res = await runGapsToIntake({ dryRun: true, forceWithoutSufficiency: true });
  assert.equal(res.ok, true, JSON.stringify(res, null, 2));
  assert.equal(res.promoted_count, 1);
  assert.equal(res.promoted[0].dry_run, true);
  assert.ok(res.promoted[0].id.startsWith("GAP_"));
});

test("--gaps-to-intake de-dupes (second run does not create a second intake)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-gaps-to-intake-knowledge-dedupe-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  mkdirSync(join(opsRoot, "ai", "lane_b"), { recursive: true });
  mkdirSync(join(opsRoot, "config"), { recursive: true });

  const projectId = "proj-gap-export-dedupe";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  writeFileSync(join(opsRoot, "config", "TEAMS.json"), JSON.stringify({ version: 1, teams: [{ team_id: "Tooling", description: "t", scope_hints: [], risk_level: "normal" }] }, null, 2) + "\n", "utf8");
  writeFileSync(
    join(opsRoot, "config", "REPOS.json"),
    JSON.stringify({ version: 1, repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }] }, null, 2) + "\n",
    "utf8",
  );

  // Make the active repo non-stale by providing a real git repo and running index+scan.
  const repoAbs = join(root, "repos", "repo-a");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('repo-a')\n", "utf8");
  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);
  assert.equal((await runKnowledgeIndex({ projectRoot: opsRoot, dryRun: false })).ok, true);
  assert.equal((await runKnowledgeScan({ projectRoot: opsRoot, dryRun: false })).ok, true);

  const gapsPath = join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "gaps.json");
  mkdirSync(join(knowledgeRepo.knowledgeRootAbs, "ssot", "system"), { recursive: true });
  writeFileSync(
    gapsPath,
    JSON.stringify(
      {
        version: 1,
        scope: "system",
        captured_at: new Date().toISOString(),
        extractor_version: "1",
        gaps: [
          {
            scope: "system",
            category: "integration_missing",
            severity: "high",
            risk: "high",
            summary: "Missing provider for endpoint GET /api/x",
            expected: "Provider exists",
            observed: "Not found",
            evidence: [{ type: "endpoint", method: "GET", path: "/api/x" }],
            suggested_intake: { repo_id: "repo-a", title: "Fix integration", body: "Add endpoint", labels: ["gap", "ai"] },
          },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  const r1 = await runGapsToIntake({ dryRun: false, forceWithoutSufficiency: true });
  assert.equal(r1.ok, true);
  assert.equal(r1.promoted_count, 1);

  const inboxDir = join(opsRoot, "ai", "lane_b", "inbox");
  assert.equal(existsSync(inboxDir), true);
  const files1 = readdirSync(inboxDir).filter((n) => n.startsWith("I-") && n.endsWith(".md"));
  assert.equal(files1.length, 1);

  const r2 = await runGapsToIntake({ dryRun: false, forceWithoutSufficiency: true });
  assert.equal(r2.ok, true);
  assert.equal(r2.promoted_count, 0);
  assert.equal(r2.skipped_count, 1);

  const files2 = readdirSync(inboxDir).filter((n) => n.startsWith("I-") && n.endsWith(".md"));
  assert.equal(files2.length, 1);
});
