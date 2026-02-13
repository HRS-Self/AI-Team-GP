import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { validateEvidenceRef, validateKnowledgeScan } from "../src/contracts/validators/index.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("index-driven knowledge-scan writes scan.json + evidence_refs.jsonl + SCAN_REPORT.md with evidence-backed facts", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-scan-index-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;

  const projectId = "proj-scan-index";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });
  mkdirSync(join(opsRoot, "config"), { recursive: true });

  const reposRoot = join(root, "repos");
  mkdirSync(reposRoot, { recursive: true });
  const repoAbs = join(reposRoot, "repo-a");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('hi')\n", "utf8");
  writeFileSync(join(repoAbs, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: X\n  version: 1\n", "utf8");

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

  const idx = await runKnowledgeIndex({ projectRoot: opsRoot, dryRun: false });
  assert.equal(idx.ok, true);

  const scan1 = await runKnowledgeScan({ projectRoot: opsRoot, dryRun: false });
  assert.equal(scan1.ok, true);
  assert.equal(scan1.scanned.length, 1);

  const scanAbs = join(knowledgeRepo.knowledgeRootAbs, "ssot", "repos", "repo-a", "scan.json");
  const refsAbs = join(knowledgeRepo.knowledgeRootAbs, "evidence", "repos", "repo-a", "evidence_refs.jsonl");
  const repAbs = join(knowledgeRepo.knowledgeRootAbs, "views", "repos", "repo-a", "SCAN_REPORT.md");
  assert.equal(existsSync(scanAbs), true);
  assert.equal(existsSync(refsAbs), true);
  assert.equal(existsSync(repAbs), true);

  const scanJson = JSON.parse(readFileSync(scanAbs, "utf8"));
  validateKnowledgeScan(scanJson);
  assert.ok(Array.isArray(scanJson.facts) && scanJson.facts.length >= 1);
  for (const f of scanJson.facts) assert.ok(Array.isArray(f.evidence_ids) && f.evidence_ids.length >= 1);

  const refsText = readFileSync(refsAbs, "utf8");
  const lines = refsText.split("\n").map((l) => l.trim()).filter(Boolean);
  assert.ok(lines.length >= 1);
  const refs = lines.map((l) => JSON.parse(l));
  for (const r of refs) validateEvidenceRef(r);
  const ids = new Set(refs.map((r) => r.evidence_id));
  for (const f of scanJson.facts) for (const id of f.evidence_ids) assert.ok(ids.has(id));

  const report = readFileSync(repAbs, "utf8");
  assert.ok(report.includes(`SCAN_REPORT: ${scanJson.repo_id}`));
  assert.ok(report.includes(`scan_version: ${scanJson.scan_version}`));

  // Determinism: rerun scan yields same scan_version.
  const scan2 = await runKnowledgeScan({ projectRoot: opsRoot, dryRun: false });
  assert.equal(scan2.ok, true);
  const scanJson2 = JSON.parse(readFileSync(scanAbs, "utf8"));
  assert.equal(scanJson2.scan_version, scanJson.scan_version);
});

test("knowledge-scan implementation does not reference LLM modules", () => {
  const text = readFileSync("src/lane_a/knowledge/knowledge-scan.js", "utf8");
  assert.equal(/createLlmClient|src\/llm\//.test(text), false);
});
