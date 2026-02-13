import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { runRepoIndex } from "../src/lane_a/knowledge/repo-indexer.js";

function run(cmd, args, cwd) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

async function initGitRepo(repoAbs) {
  assert.ok(run("git", ["init", "-q"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.email", "t@example.com"], repoAbs).ok);
  assert.ok(run("git", ["config", "user.name", "t"], repoAbs).ok);
  assert.ok(run("git", ["add", "."], repoAbs).ok);
  assert.ok(run("git", ["commit", "-m", "init"], repoAbs).ok);
}

test("repo-indexer detects Java/Kotlin entrypoints and build system files", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-repo-indexer-java-"));
  const repoAbs = join(root, "repo-jvm");
  mkdirSync(join(repoAbs, "src", "main", "java", "com", "example"), { recursive: true });
  writeFileSync(join(repoAbs, "pom.xml"), "<project></project>\n", "utf8");
  writeFileSync(join(repoAbs, "src", "main", "java", "com", "example", "DemoApplication.java"), "public class DemoApplication {}\n", "utf8");
  await initGitRepo(repoAbs);

  const outDir = join(root, "out");
  const res = await runRepoIndex({ repo_id: "repo-jvm", repo_path: repoAbs, output_dir: outDir, dry_run: false });
  assert.equal(res.ok, true);
  const idx = JSON.parse(readFileSync(join(outDir, "repo_index.json"), "utf8"));
  assert.ok(idx.entrypoints.includes("pom.xml"));
  assert.ok(idx.entrypoints.some((p) => p.endsWith("DemoApplication.java")));
  assert.equal(idx.build_commands.package_manager, "maven");
  assert.ok(idx.build_commands.evidence_files.includes("pom.xml"));
});

test("repo-indexer detects .NET entrypoints and project files", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-repo-indexer-dotnet-"));
  const repoAbs = join(root, "repo-dotnet");
  mkdirSync(repoAbs, { recursive: true });
  writeFileSync(join(repoAbs, "Program.cs"), "Console.WriteLine(\"hi\");\n", "utf8");
  writeFileSync(join(repoAbs, "App.csproj"), "<Project></Project>\n", "utf8");
  await initGitRepo(repoAbs);

  const outDir = join(root, "out");
  const res = await runRepoIndex({ repo_id: "repo-dotnet", repo_path: repoAbs, output_dir: outDir, dry_run: false });
  assert.equal(res.ok, true);
  const idx = JSON.parse(readFileSync(join(outDir, "repo_index.json"), "utf8"));
  assert.ok(idx.entrypoints.includes("Program.cs"));
  assert.ok(idx.entrypoints.includes("App.csproj"));
  assert.equal(idx.build_commands.package_manager, "dotnet");
});

