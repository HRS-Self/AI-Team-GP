import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runKnowledgeScan } from "../src/lane_a/knowledge/knowledge-scan.js";
import { runKnowledgeDepsApprove } from "../src/lane_a/knowledge/knowledge-deps-approve.js";
import { withRegistryLock, loadRegistry, upsertProject, writeRegistry } from "../src/registry/project-registry.js";

function run(cmd, args, cwd, env = null) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8", env: env ? { ...process.env, ...env } : process.env });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

async function upsertExternalProjectInRegistry({ registryDirAbs, projectCode, externalHomeAbs, repoId }) {
  const restore = process.env.AI_TEAM_REGISTRY_DIR;
  process.env.AI_TEAM_REGISTRY_DIR = registryDirAbs;
  try {
    await withRegistryLock(async () => {
      const regRes = await loadRegistry({ createIfMissing: true });
      const reg = regRes.registry;

      const now = "2026-02-10T00:00:00.000Z";
      const proj = {
        project_code: projectCode,
        status: "active",
        root_dir: externalHomeAbs,
        ops_dir: join(externalHomeAbs, "ops"),
        repos_dir: join(externalHomeAbs, "repos"),
        created_at: now,
        updated_at: now,
        ports: { webui_port: 9000, websvc_port: 9001 },
        pm2: { ecosystem_path: join(externalHomeAbs, "ops", "pm2", "ecosystem.config.cjs"), apps: [`${projectCode}-webui`, `${projectCode}-websvc`] },
        cron: { installed: false, entries: [] },
        knowledge: { type: "git", abs_path: join(externalHomeAbs, "knowledge"), git_remote: "", default_branch: "main", active_branch: "main", last_commit_sha: null },
        repos: [
          {
            repo_id: repoId,
            owner_repo: `Org/${repoId}`,
            abs_path: join(externalHomeAbs, "repos", repoId),
            default_branch: "main",
            active_branch: "main",
            last_seen_head_sha: null,
            active: true,
          },
        ],
      };

      upsertProject(reg, proj);
      await writeRegistry(reg);
    });
  } finally {
    if (restore === undefined) delete process.env.AI_TEAM_REGISTRY_DIR;
    else process.env.AI_TEAM_REGISTRY_DIR = restore;
  }
}

test("dependency graph: index writes graph+override; scan blocks until approved; external bundle reuse enforced", async () => {
  const hostRoot = mkdtempSync(join(tmpdir(), "ai-team-deps-"));
  const registryDirAbs = join(hostRoot, "ai-team-registry");
  process.env.AI_TEAM_REGISTRY_DIR = registryDirAbs;

  const projectHomeAbs = join(hostRoot, "proj-a");
  const projectId = "proj-a";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: projectHomeAbs, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: projectHomeAbs, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const opsRootAbs = join(projectHomeAbs, "ops");
  process.env.AI_PROJECT_ROOT = opsRootAbs;

  const repoId = "repo-a";
  const reposRootAbs = join(projectHomeAbs, "repos");
  const repoAbs = join(reposRootAbs, repoId);
  mkdirSync(repoAbs, { recursive: true });
  writeFileSync(join(repoAbs, "README.md"), "# repo-a\n", "utf8");
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", version: "1.0.0", dependencies: { hivejs: "1.0.0" } }, null, 2) + "\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

  writeFileSync(
    join(opsRootAbs, "config", "REPOS.json"),
    JSON.stringify({ version: 1, repos: [{ repo_id: repoId, path: repoId, status: "active", team_id: "Tooling" }] }, null, 2) + "\n",
    "utf8",
  );

  const externalHomeAbs = join(hostRoot, "hive-proj");
  initKnowledgeRepoWithMinimalSsot({ projectRoot: externalHomeAbs, projectId: "hive-proj", activeTeams: ["Tooling"], sharedPacks: [] });
  await upsertExternalProjectInRegistry({ registryDirAbs, projectCode: "HiveJS", externalHomeAbs, repoId: "hivejs" });

  const idx = await runKnowledgeIndex({ projectRoot: opsRootAbs, dryRun: false });
  assert.equal(idx.ok, true, JSON.stringify(idx, null, 2));

  const depGraphAbs = join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "dependency_graph.json");
  const depOverrideAbs = join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "dependency_graph.override.json");
  assert.equal(existsSync(depGraphAbs), true);
  assert.equal(existsSync(depOverrideAbs), true);
  const override = JSON.parse(readFileSync(depOverrideAbs, "utf8"));
  assert.equal(override.status, "pending");

  const repoIndexAbs = join(knowledgeRepo.knowledgeRootAbs, "evidence", "index", "repos", repoId, "repo_index.json");
  const repoIndex = JSON.parse(readFileSync(repoIndexAbs, "utf8"));
  assert.equal(Array.isArray(repoIndex.dependencies.depends_on), true);
  assert.equal(repoIndex.dependencies.depends_on.length, 1);
  assert.equal(repoIndex.dependencies.depends_on[0].project_code, "HiveJS");
  assert.equal(repoIndex.dependencies.depends_on[0].repo_id, "hivejs");

  const scanBlocked = await runKnowledgeScan({ projectRoot: opsRootAbs, concurrency: 1, dryRun: false });
  assert.equal(scanBlocked.ok, false);
  assert.ok(String(scanBlocked.message || "").toLowerCase().includes("dependencies not approved"));
  const blockerAbs = join(opsRootAbs, "ai", "lane_a", "blockers", "DEPS_NOT_APPROVED.json");
  assert.equal(existsSync(blockerAbs), true);

  const approved = await runKnowledgeDepsApprove({ projectRoot: opsRootAbs, by: "Alice", notes: "ok", dryRun: false });
  assert.equal(approved.ok, true, JSON.stringify(approved, null, 2));
  const override2 = JSON.parse(readFileSync(depOverrideAbs, "utf8"));
  assert.equal(override2.status, "approved");

  const scanMissing = await runKnowledgeScan({ projectRoot: opsRootAbs, concurrency: 1, dryRun: false });
  assert.equal(scanMissing.ok, false);
  assert.equal(scanMissing.failed.length, 1);
  assert.ok(String(scanMissing.failed[0].message || "").includes("external_dependency_bundle_missing"));

  // Provide minimal external knowledge artifacts for hivejs so scan can load dependency context.
  const extK = join(externalHomeAbs, "knowledge");
  mkdirSync(join(extK, "ssot", "repos", "hivejs"), { recursive: true });
  mkdirSync(join(extK, "evidence", "repos", "hivejs"), { recursive: true });
  mkdirSync(join(extK, "evidence", "index", "repos", "hivejs"), { recursive: true });

  writeFileSync(
    join(extK, "ssot", "repos", "hivejs", "scan.json"),
    JSON.stringify(
      {
        repo_id: "hivejs",
        scanned_at: "20260210_000000000",
        scan_version: 1,
        external_knowledge: [],
        facts: [{ fact_id: "FACT_EXT", claim: "Entrypoint: README.md", evidence_ids: ["EVID_EXT"] }],
        unknowns: [],
        contradictions: [],
        coverage: { files_seen: 1, files_indexed: 1 },
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );
  writeFileSync(
    join(extK, "evidence", "repos", "hivejs", "evidence_refs.jsonl"),
    `${JSON.stringify({
      evidence_id: "EVID_EXT",
      repo_id: "hivejs",
      file_path: "README.md",
      commit_sha: "a".repeat(40),
      start_line: 1,
      end_line: 1,
      extractor: "test",
      captured_at: "2026-02-10T00:00:00.000Z",
    })}\n`,
    "utf8",
  );
  writeFileSync(
    join(extK, "evidence", "index", "repos", "hivejs", "repo_index.json"),
    JSON.stringify(
      {
        version: 1,
        repo_id: "hivejs",
        scanned_at: "2026-02-10T00:00:00.000Z",
        head_sha: "a".repeat(40),
        languages: [],
        entrypoints: ["README.md"],
        build_commands: { package_manager: "npm", install: [], lint: [], build: [], test: [], scripts: {}, evidence_files: [] },
        api_surface: { openapi_files: [], routes_controllers: [], events_topics: [] },
        migrations_schema: [],
        cross_repo_dependencies: [],
        hotspots: [],
        fingerprints: { "README.md": { sha256: "a".repeat(64) } },
        dependencies: { version: 1, detected_at: "2026-02-10T00:00:00.000Z", mode: "detected", depends_on: [] },
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );
  writeFileSync(
    join(extK, "evidence", "index", "repos", "hivejs", "repo_fingerprints.json"),
    JSON.stringify({ repo_id: "hivejs", captured_at: "2026-02-10T00:00:00.000Z", files: [] }, null, 2) + "\n",
    "utf8",
  );

  const scanOk = await runKnowledgeScan({ projectRoot: opsRootAbs, concurrency: 1, dryRun: false });
  assert.equal(scanOk.ok, true, JSON.stringify(scanOk, null, 2));
  assert.equal(scanOk.scanned.length, 1);

  const outScanAbs = join(knowledgeRepo.knowledgeRootAbs, "ssot", "repos", repoId, "scan.json");
  const outScan = JSON.parse(readFileSync(outScanAbs, "utf8"));
  assert.equal(Array.isArray(outScan.external_knowledge), true);
  assert.equal(outScan.external_knowledge.length, 1);
  assert.equal(outScan.external_knowledge[0].project_code, "HiveJS");
  assert.equal(outScan.external_knowledge[0].repo_id, "hivejs");
  assert.ok(String(outScan.external_knowledge[0].bundle_id || "").startsWith("sha256-"));
  assert.equal(outScan.external_knowledge[0].path, join(extK, "ssot", "repos", "hivejs", "scan.json"));
  assert.equal(outScan.external_knowledge[0].loaded_at, "2026-02-10T00:00:00.000Z");

  // Audit entry exists.
  const decisionsDirAbs = join(knowledgeRepo.knowledgeRootAbs, "ssot", "system", "decisions");
  const decisionFiles = existsSync(decisionsDirAbs) ? readdirSync(decisionsDirAbs).filter((f) => f.startsWith("dependency_approval_") && f.endsWith(".json")) : [];
  assert.ok(decisionFiles.length >= 1);
});
