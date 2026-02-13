import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, mkdirSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

import { runInitialProjectOnboarding } from "../src/onboarding/onboarding-runner.js";

function sh(cmd, { cwd }) {
  const r = spawnSync("bash", ["-lc", cmd], { cwd, encoding: "utf8" });
  if (r.status !== 0) throw new Error(`Command failed (${r.status}): ${cmd}\n${String(r.stderr || r.stdout || "")}`);
  return String(r.stdout || "").trim();
}

function initBareRemoteWithDevelopDefault({ root, name }) {
  const bare = join(root, `${name}.git`);
  const work = join(root, `${name}-work`);
  mkdirSync(work, { recursive: true });

  sh("git init -q", { cwd: work });
  sh('git config user.email "test@example.com"', { cwd: work });
  sh('git config user.name "Test"', { cwd: work });

  // Initial commit on main (older).
  writeFileSync(join(work, "README.md"), "# repo\n", "utf8");
  sh('GIT_AUTHOR_DATE="2020-01-01T00:00:00Z" GIT_COMMITTER_DATE="2020-01-01T00:00:00Z" git add -A', { cwd: work });
  sh('GIT_AUTHOR_DATE="2020-01-01T00:00:00Z" GIT_COMMITTER_DATE="2020-01-01T00:00:00Z" git commit -q -m "init"', { cwd: work });

  // Develop branch with CI + Node commands (newer).
  sh("git checkout -q -b develop", { cwd: work });
  mkdirSync(join(work, ".github", "workflows"), { recursive: true });
  writeFileSync(join(work, ".github", "workflows", "ci.yml"), "name: CI\n", "utf8");
  writeFileSync(
    join(work, "package.json"),
    JSON.stringify({ name: "code-repo", version: "1.0.0", scripts: { lint: "echo lint", test: "echo test", build: "echo build" } }, null, 2) + "\n",
    "utf8",
  );
  writeFileSync(join(work, "package-lock.json"), "{}", "utf8");
  sh('GIT_AUTHOR_DATE="2021-01-01T00:00:00Z" GIT_COMMITTER_DATE="2021-01-01T00:00:00Z" git add -A', { cwd: work });
  sh('GIT_AUTHOR_DATE="2021-01-01T00:00:00Z" GIT_COMMITTER_DATE="2021-01-01T00:00:00Z" git commit -q -m "develop setup"', { cwd: work });

  // Create bare + push.
  sh(`git init --bare -q ${JSON.stringify(bare)}`, { cwd: root });
  sh(`git remote add origin ${JSON.stringify(bare)}`, { cwd: work });
  sh("git push -q -u origin develop", { cwd: work });
  sh("git push -q origin HEAD:main", { cwd: work });
  sh(`git --git-dir=${JSON.stringify(bare)} symbolic-ref HEAD refs/heads/develop`, { cwd: root });

  return bare;
}

test("initial-project scanner generates TEAMS/AGENTS and infers repo active_branch/commands", async () => {
  const tmp = mkdtempSync(join(tmpdir(), "ai-team-onboard-scan-"));
  const regDir = mkdtempSync(join(tmpdir(), "ai-team-onboard-reg-"));
  const projectsRoot = mkdtempSync(join(tmpdir(), "ai-team-onboard-projects-"));
  const toolAbs = resolve(dirname(fileURLToPath(import.meta.url)), "..");

  const bareCode = initBareRemoteWithDevelopDefault({ root: tmp, name: "code-repo" });

  process.env.AI_TEAM_REGISTRY_DIR = regDir;
  process.env.AI_TEAM_PROJECTS_ROOT = projectsRoot;
  process.env.AI_TEAM_CODE_REPO_PATHS = bareCode;

  try {
    const system = {
      git: {
        initIfMissing(repoAbs) {
          // idempotent: do nothing if already cloned/initialized
          const dotgit = resolve(repoAbs, ".git");
          if (existsSync(dotgit)) return { ok: true, created: false };
          const r = spawnSync("git", ["init", "-q"], { cwd: repoAbs, encoding: "utf8" });
          return { ok: r.status === 0, created: r.status === 0 };
        },
        ensureSafeDirectory() {
          return { ok: true, wrote: false };
        },
        ensureOriginIfProvided() {
          return { ok: true, remote: "", default_branch: "main", created: false };
        },
        headSha() {
          return null;
        },
      },
      cron: {
        installBlock({ entries }) {
          return { ok: true, installed: true, entries };
        },
      },
    };

    const res = await runInitialProjectOnboarding({
      toolRepoRoot: toolAbs,
      dryRun: false,
      project: "alpha",
      nonInteractive: true,
      system,
    });
    assert.equal(res.ok, true);

    const ops = join(projectsRoot, "alpha", "ops");
    const teams = JSON.parse(readFileSync(join(ops, "config", "TEAMS.json"), "utf8"));
    const agents = JSON.parse(readFileSync(join(ops, "config", "AGENTS.json"), "utf8"));
    const repos = JSON.parse(readFileSync(join(ops, "config", "REPOS.json"), "utf8"));

    assert.equal(teams.version, 1);
    assert.ok(Array.isArray(teams.teams) && teams.teams.length > 0);
    assert.ok(teams.teams.some((t) => t && t.team_id === "QA"), "TEAMS.json must include QA team");

    assert.equal(agents.version, 3);
    assert.ok(Array.isArray(agents.agents) && agents.agents.some((a) => a.role === "planner" && a.implementation === "llm"));
    assert.ok(agents.agents.some((a) => a && a.team_id === "QA" && a.role === "qa_inspector" && a.implementation === "llm"), "AGENTS.json must include qa_inspector LLM for QA");

    assert.equal(repos.version, 1);
    assert.ok(Array.isArray(repos.repos) && repos.repos.length === 1);
    assert.equal(repos.repos[0].repo_id, "code-repo");
    assert.equal(repos.repos[0].active_branch, "develop");
    assert.ok(typeof repos.repos[0].commands?.install === "string" && repos.repos[0].commands.install.length > 0);
  } finally {
    delete process.env.AI_TEAM_REGISTRY_DIR;
    delete process.env.AI_TEAM_PROJECTS_ROOT;
    delete process.env.AI_TEAM_CODE_REPO_PATHS;
  }
});
