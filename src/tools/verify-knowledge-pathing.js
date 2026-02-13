import { mkdtemp, rm, writeFile, readdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

function sh(cwd, cmd, env = {}) {
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8", env: { ...process.env, ...env } });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

async function listFilesRecursive(dirAbs) {
  const out = [];
  async function walk(cur) {
    const entries = await readdir(cur, { withFileTypes: true });
    for (const e of entries) {
      const p = join(cur, e.name);
      if (e.isDirectory()) await walk(p);
      else out.push(p);
    }
  }
  if (!existsSync(dirAbs)) return [];
  await walk(dirAbs);
  return out.sort((a, b) => a.localeCompare(b));
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

async function main() {
  const base = await mkdtemp(join(tmpdir(), "ai-team-knowledge-"));
  const projectHomeAbs = resolve(join(base, "project"));
  const opsRootAbs = resolve(join(projectHomeAbs, "ops"));
  const reposRootAbs = resolve(join(projectHomeAbs, "repos"));
  const knowledgeRootAbs = resolve(join(projectHomeAbs, "knowledge"));
  const originBare = resolve(join(base, "origin.git"));

  try {
    // Create bare origin and a working knowledge repo with origin set.
    assert(sh(base, `git init --bare "${originBare}"`).ok, "Failed to init bare origin.");
    assert(sh(base, `mkdir -p "${opsRootAbs}/config" "${opsRootAbs}/ai/lane_b" "${reposRootAbs}" "${knowledgeRootAbs}"`).ok, "Failed to mkdir project roots.");
    assert(sh(knowledgeRootAbs, "git init").ok, "Failed to git init knowledge repo.");
    assert(sh(knowledgeRootAbs, "git checkout -b main").ok, "Failed to create main branch in knowledge repo.");
    assert(sh(knowledgeRootAbs, "git config user.email \"verify@example.com\"").ok, "Failed to set git user.email.");
    assert(sh(knowledgeRootAbs, "git config user.name \"AI-Team Verify\"").ok, "Failed to set git user.name.");
    assert(sh(knowledgeRootAbs, `git remote add origin "${originBare}"`).ok, "Failed to set origin remote.");
    await writeFile(join(knowledgeRootAbs, "README.md"), "# Knowledge Repo (test)\n", "utf8");
    assert(sh(knowledgeRootAbs, "git add README.md").ok, "Failed to git add README.md.");
    assert(sh(knowledgeRootAbs, "git commit -m \"init\"").ok, "Failed to git commit init.");
    assert(sh(knowledgeRootAbs, "git push -u origin main").ok, "Failed to git push init.");

    // Write OPS config (hard contract v1).
    await writeFile(
      join(opsRootAbs, "config", "PROJECT.json"),
      JSON.stringify(
        {
          version: 4,
          project_code: "verify",
          repos_root_abs: reposRootAbs,
          ops_root_abs: opsRootAbs,
          knowledge_repo_dir: knowledgeRootAbs,
          ssot_bundle_policy: { global_packs: [] },
        },
        null,
        2,
      ) + "\n",
      "utf8",
    );
    await writeFile(join(opsRootAbs, "config", "REPOS.json"), JSON.stringify({ version: 1, repos: [] }, null, 2) + "\n", "utf8");
    await writeFile(
      join(opsRootAbs, "config", "TEAMS.json"),
      JSON.stringify({ version: 1, teams: [{ team_id: "Tooling", description: "verify", scope_hints: ["verify"], risk_level: "normal" }] }, null, 2) + "\n",
      "utf8",
    );
    await writeFile(
      join(opsRootAbs, "config", "POLICIES.json"),
      JSON.stringify({ version: 1, merge_strategy: "deep_merge", selectors: [], named: {} }, null, 2) + "\n",
      "utf8",
    );
    await writeFile(
      join(opsRootAbs, "config", "LLM_PROFILES.json"),
      JSON.stringify({ version: 1, profiles: { "architect.interviewer": { provider: "openai", model: "stub" } } }, null, 2) + "\n",
      "utf8",
    );
    await writeFile(
      join(opsRootAbs, "config", "AGENTS.json"),
      JSON.stringify(
        {
          version: 3,
          agents: [{ agent_id: "Tooling__interviewer__01", team_id: "Tooling", role: "interviewer", implementation: "llm", llm_profile: "architect.interviewer", capacity: 1, enabled: true }],
        },
        null,
        2,
      ) + "\n",
      "utf8",
    );
    await writeFile(join(opsRootAbs, "ai", "lane_b", "DECISIONS_NEEDED.md"), "", "utf8");

    const env = { ...process.env, AI_PROJECT_ROOT: opsRootAbs };

    // Dry-run: must not write canonical notes into ops runtime folder.
    {
      const res = sh(
        "/opt/GitRepos/AI-Team",
        `node src/cli.js --knowledge-interview --projectRoot "${opsRootAbs}" --scope system --continue --session "answers" --dry-run`,
        { ...env, KNOWLEDGE_TEST_STUB: "1" },
      );
      assert(res.ok, `Dry-run knowledge interview failed:\n${res.stderr || res.stdout}`);
      const runtimeKnowledgeFiles = await listFilesRecursive(join(opsRootAbs, "ai", "knowledge"));
      assert(runtimeKnowledgeFiles.length === 0, `Dry-run should not write runtime knowledge pointers. Found:\n${runtimeKnowledgeFiles.join("\n")}`);
    }

    // Non-dry-run: must write canonical notes into knowledge repo and only lane-specific runtime logs/checkpoints.
    {
      const res = sh(
        "/opt/GitRepos/AI-Team",
        `node src/cli.js --knowledge-interview --projectRoot "${opsRootAbs}" --scope system --continue --session "answers"`,
        { ...env, KNOWLEDGE_TEST_STUB: "1" },
      );
      assert(res.ok, `Non-dry-run knowledge interview failed:\n${res.stderr || res.stdout}`);

      // Canonical notes exist in knowledge repo.
      const sessionsDir = join(knowledgeRootAbs, "sessions");
      const mergedPath = join(knowledgeRootAbs, "ssot", "system", "assumptions.json");
      assert(existsSync(sessionsDir), "Expected knowledge/sessions dir to exist.");
      assert(existsSync(mergedPath), "Expected ssot/system/assumptions.json to exist.");
      assert(existsSync(join(knowledgeRootAbs, "ssot", "system", "BACKLOG_SEEDS.json")), "Expected ssot/system/BACKLOG_SEEDS.json to exist.");
      assert(existsSync(join(knowledgeRootAbs, "ssot", "system", "GAPS.json")), "Expected ssot/system/GAPS.json to exist.");

      // A commit exists (and was pushed to origin).
      const log = sh(knowledgeRootAbs, "git log -1 --format=%s");
      assert(log.ok, "Expected git log to succeed in knowledge repo.");
      assert(log.stdout.toLowerCase().includes("knowledge(system): session"), `Expected commit subject to include knowledge session. Got: ${log.stdout.trim()}`);
      assert(sh(knowledgeRootAbs, "git fetch origin --prune").ok, "Expected git fetch to work.");

      // Lane A must not write runtime pointer files under ai/knowledge/.
      const runtimeKnowledgeFiles = await listFilesRecursive(join(opsRootAbs, "ai", "knowledge"));
      assert(runtimeKnowledgeFiles.length === 0, `Runtime knowledge folder must remain empty. Found:\n${runtimeKnowledgeFiles.join("\n")}`);
    }

    process.stdout.write("verify:knowledge: ok\n");
  } finally {
    try {
      await rm(base, { recursive: true, force: true });
    } catch {
      // ignore
    }
  }
}

main().catch((err) => {
  process.stderr.write(`verify:knowledge: failed: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});

