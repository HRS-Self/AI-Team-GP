import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { runKnowledgeIndex } from "../src/lane_a/knowledge/knowledge-index.js";
import { runRepoIndex } from "../src/lane_a/knowledge/repo-indexer.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

test("knowledge-index produces deterministic repo_index.json and repo_fingerprints.json under knowledge/evidence/index", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-index-"));
  const opsRoot = join(root, "ops");
  process.env.AI_PROJECT_ROOT = opsRoot;
  mkdirSync(join(opsRoot, "config"), { recursive: true });

  const projectId = "proj-knowledge-index";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const reposRoot = join(root, "repos");
  mkdirSync(reposRoot, { recursive: true });
  const repoAbs = join(reposRoot, "repo-a");
  mkdirSync(join(repoAbs, "src", "routes"), { recursive: true });
  mkdirSync(join(repoAbs, "prisma", "migrations", "001_init"), { recursive: true });
  mkdirSync(join(repoAbs, "events"), { recursive: true });
  writeFileSync(
    join(repoAbs, "package.json"),
    JSON.stringify(
      { name: "repo-a", main: "src/index.js", scripts: { build: "echo build", test: "echo test", lint: "echo lint" }, dependencies: { "x-internal": "git+https://example.com/x.git" } },
      null,
      2,
    ) + "\n",
    "utf8",
  );
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('hi')\n", "utf8");
  writeFileSync(join(repoAbs, "src", "routes", "r.js"), "export const r = 1;\n", "utf8");
  writeFileSync(join(repoAbs, "openapi.yaml"), "openapi: 3.0.0\ninfo:\n  title: X\n  version: 1\n", "utf8");
  writeFileSync(join(repoAbs, "prisma", "schema.prisma"), "datasource db { provider = \"sqlite\" url = \"file:dev.db\" }\n", "utf8");
  writeFileSync(join(repoAbs, "prisma", "migrations", "001_init", "migration.sql"), "-- init\n", "utf8");
  writeFileSync(join(repoAbs, "events", "topics.md"), "# topics\n", "utf8");

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

  const r1 = await runKnowledgeIndex({ projectRoot: opsRoot, dryRun: false });
  assert.equal(r1.ok, true);
  const dir = join(knowledgeRepo.knowledgeRootAbs, "evidence", "index", "repos", "repo-a");
  const idx1Path = join(dir, "repo_index.json");
  const fp1Path = join(dir, "repo_fingerprints.json");
  const md1Path = join(dir, "repo_index.md");
  assert.equal(existsSync(idx1Path), true);
  assert.equal(existsSync(fp1Path), true);
  assert.equal(existsSync(md1Path), true);

  const idx1 = readFileSync(idx1Path, "utf8");
  const fp1 = readFileSync(fp1Path, "utf8");
  const md1 = readFileSync(md1Path, "utf8");

  const r2 = await runKnowledgeIndex({ projectRoot: opsRoot, dryRun: false });
  assert.equal(r2.ok, true);
  assert.equal(readFileSync(idx1Path, "utf8"), idx1);
  assert.equal(readFileSync(fp1Path, "utf8"), fp1);
  assert.equal(readFileSync(md1Path, "utf8"), md1);

  const parsed = JSON.parse(idx1);
  assert.ok(Array.isArray(parsed.entrypoints));
  assert.ok(parsed.entrypoints.includes("src/index.js"));
  assert.equal(typeof parsed.build_commands, "object");
  assert.equal(parsed.build_commands.package_manager, "npm");
  assert.ok(Array.isArray(parsed.build_commands.build));
  assert.ok(parsed.build_commands.build.some((c) => c.includes("run build")));
  assert.equal(typeof parsed.api_surface, "object");
  assert.ok(Array.isArray(parsed.api_surface.openapi_files));
  assert.ok(parsed.api_surface.openapi_files.includes("openapi.yaml"));
  assert.ok(Array.isArray(parsed.api_surface.routes_controllers));
  assert.ok(parsed.api_surface.routes_controllers.includes("src/routes/r.js"));
  assert.ok(Array.isArray(parsed.migrations_schema));
  assert.ok(parsed.migrations_schema.includes("prisma/schema.prisma"));
  assert.ok(Array.isArray(parsed.cross_repo_dependencies));
  assert.ok(parsed.cross_repo_dependencies.some((d) => d.type === "git" && String(d.target).includes("x-internal@")));
});

test("repo_fingerprints change when a fingerprinted file changes (new commit)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-index-change-"));
  const repoAbs = join(root, "repo-a");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('hi')\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

  const outDir = join(root, "out");
  const a1 = await runRepoIndex({ repo_id: "repo-a", repo_path: repoAbs, output_dir: outDir, dry_run: false });
  assert.equal(a1.ok, true);
  const fp1 = JSON.parse(readFileSync(join(outDir, "repo_fingerprints.json"), "utf8"));
  const pkg1 = fp1.files.find((f) => f.path === "package.json");
  assert.ok(pkg1);

  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js", version: "2" }, null, 2) + "\n", "utf8");
  assert.ok(run("git", ["add", "package.json"], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "bump"], repoAbs).ok);

  const a2 = await runRepoIndex({ repo_id: "repo-a", repo_path: repoAbs, output_dir: outDir, dry_run: false });
  assert.equal(a2.ok, true);
  const fp2 = JSON.parse(readFileSync(join(outDir, "repo_fingerprints.json"), "utf8"));
  const pkg2 = fp2.files.find((f) => f.path === "package.json");
  assert.ok(pkg2);
  assert.notEqual(pkg2.sha256, pkg1.sha256);
});

test("repo-indexer fails fast on repo with no fingerprintable files", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-index-invalid-"));
  const repoAbs = join(root, "repo-x");
  mkdirSync(repoAbs, { recursive: true });
  writeFileSync(join(repoAbs, "notes.txt"), "x\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

  const outDir = join(root, "out");
  const res = await runRepoIndex({ repo_id: "repo-x", repo_path: repoAbs, output_dir: outDir, dry_run: false });
  assert.equal(res.ok, false);
  assert.equal(existsSync(join(outDir, "repo_index.json")), false);
  assert.equal(existsSync(join(outDir, "repo_fingerprints.json")), false);
  assert.equal(existsSync(join(outDir, "knowledge-index__repo-x.error.json")), true);
});

test("repo_index.md reflects repo_index.json content", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-knowledge-index-md-"));
  const repoAbs = join(root, "repo-a");
  mkdirSync(join(repoAbs, "src"), { recursive: true });
  writeFileSync(join(repoAbs, "package.json"), JSON.stringify({ name: "repo-a", main: "src/index.js" }, null, 2) + "\n", "utf8");
  writeFileSync(join(repoAbs, "src", "index.js"), "console.log('hi')\n", "utf8");

  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);

  const outDir = join(root, "out");
  const res = await runRepoIndex({ repo_id: "repo-a", repo_path: repoAbs, output_dir: outDir, dry_run: false });
  assert.equal(res.ok, true);
  const idx = JSON.parse(readFileSync(join(outDir, "repo_index.json"), "utf8"));
  const md = readFileSync(join(outDir, "repo_index.md"), "utf8");
  for (const ep of idx.entrypoints) assert.ok(md.includes(ep));
  assert.ok(md.includes(`repo_id: ${idx.repo_id}`));
});

test("repo-indexer implementation does not import or reference LLM modules", () => {
  const text = readFileSync("src/lane_a/knowledge/repo-indexer.js", "utf8");
  assert.equal(/createLlmClient|src\/llm\//.test(text), false);
});
