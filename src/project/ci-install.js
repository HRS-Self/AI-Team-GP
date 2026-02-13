import { readFileSync, existsSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve, join } from "node:path";
import { spawnSync } from "node:child_process";

import { loadRepoRegistry, resolveRepoAbsPath } from "../utils/repo-registry.js";
import { assertGhReady, createPr, prNumberFromUrl, addPrLabel } from "../github/gh.js";
import { parseGitHubOwnerRepo } from "../integrations/github-actions.js";

function nowISO() {
  return new Date().toISOString();
}

function runGit(cwd, args) {
  const argv = Array.isArray(args) ? args.map((a) => String(a)) : [];
  const res = spawnSync("git", argv, { cwd, shell: false, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || ""), cmd: `git ${argv.join(" ")}` };
}

function mustBeGitRepo(repoAbs) {
  const res = runGit(repoAbs, ["rev-parse", "--is-inside-work-tree"]);
  if (!res.ok || res.stdout.trim() !== "true") throw new Error(`Target repo is not a git worktree: ${repoAbs}`);
}

function requireCleanWorktree(repoAbs) {
  const st = runGit(repoAbs, ["status", "--porcelain"]);
  if (!st.ok) throw new Error(`Failed to check git status: ${repoAbs}`);
  if (st.stdout.trim()) throw new Error(`Refuse to proceed: repo has uncommitted changes: ${repoAbs}`);
}

function detectBaseBranch({ repoConfig, repoAbs }) {
  const active = typeof repoConfig?.active_branch === "string" && repoConfig.active_branch.trim() ? repoConfig.active_branch.trim() : null;
  if (active) return { ok: true, branch: active, method: "active_branch" };

  runGit(repoAbs, ["fetch", "origin", "--prune"]);
  const head = runGit(repoAbs, ["symbolic-ref", "refs/remotes/origin/HEAD"]);
  if (head.ok) {
    const m = head.stdout.trim().match(/^refs\/remotes\/origin\/(.+?)\s*$/);
    if (m && m[1]) return { ok: true, branch: m[1].trim(), method: "origin_head" };
  }
  return { ok: false, message: "Cannot determine base branch (set repos[].active_branch or configure origin/HEAD)." };
}

function loadWorkflowTemplateText() {
  const p = resolve("src/templates/workflows/ai-team-ci.yml");
  if (!existsSync(p)) throw new Error(`Missing workflow template: ${p}`);
  return readFileSync(p, "utf8");
}

function parseRepoFullNameFromOrigin(repoAbs) {
  const remote = runGit(repoAbs, ["remote", "get-url", "origin"]);
  if (!remote.ok) return null;
  const parsed = parseGitHubOwnerRepo(remote.stdout.trim());
  if (!parsed) return null;
  return `${parsed.owner}/${parsed.repo}`;
}

export async function runCiInstall({ repoId, branch = null, commit = false, dryRun = false } = {}) {
  assertGhReady();
  if (!repoId || !String(repoId).trim()) return { ok: false, message: "Missing --repo <repo_id>." };

  const loaded = await loadRepoRegistry();
  if (!loaded.ok) return { ok: false, message: loaded.message };
  const registry = loaded.registry;

  const repo = (registry.repos || []).find((r) => String(r?.repo_id || "").trim() === String(repoId).trim()) || null;
  if (!repo) return { ok: false, message: `Unknown repo_id in config/REPOS.json: ${repoId}` };

  const repoAbs = resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: repo.path });
  if (!repoAbs) return { ok: false, message: `Repo path missing for ${repoId}` };
  if (!existsSync(repoAbs)) return { ok: false, message: `Repo directory missing: ${repoAbs}` };

  mustBeGitRepo(repoAbs);

  const workflowText = loadWorkflowTemplateText();
  const workflowRelPath = ".github/workflows/ai-team-ci.yml";
  const workflowAbsPath = join(repoAbs, workflowRelPath);
  const workflowsDir = join(repoAbs, ".github", "workflows");

  const result = {
    ok: true,
    repo_id: String(repoId).trim(),
    repo_path: repoAbs,
    wrote_file: false,
    workflow_path: workflowAbsPath,
    commit: null,
    pr: null,
    dry_run: !!dryRun,
  };

  if (dryRun) return { ...result, ok: true, would_write: workflowAbsPath, would_commit: !!commit, branch: branch || "ai/ci-ai-team" };

  if (!commit) {
    await mkdir(workflowsDir, { recursive: true });
    await writeFile(workflowAbsPath, workflowText, "utf8");
    result.wrote_file = true;
    return result;
  }

  // Commit mode: require a clean repo, then switch branch first, then write file.
  requireCleanWorktree(repoAbs);

  const baseRes = detectBaseBranch({ repoConfig: repo, repoAbs });
  if (!baseRes.ok) return { ok: false, message: baseRes.message };
  const baseBranch = baseRes.branch;

  const workBranch = branch && String(branch).trim() ? String(branch).trim() : "ai/ci-ai-team";

  // Ensure local base is up to date.
  runGit(repoAbs, ["fetch", "origin", "--prune"]);
  const switchRes = runGit(repoAbs, ["switch", "-C", workBranch, `origin/${baseBranch}`]);
  if (!switchRes.ok) return { ok: false, message: `Failed to create/switch branch '${workBranch}': ${switchRes.stderr.trim() || switchRes.stdout.trim()}` };

  await mkdir(workflowsDir, { recursive: true });
  await writeFile(workflowAbsPath, workflowText, "utf8");
  result.wrote_file = true;

  const addRes = runGit(repoAbs, ["add", workflowRelPath]);
  if (!addRes.ok) return { ok: false, message: `Failed to git add ${workflowRelPath}: ${addRes.stderr.trim() || addRes.stdout.trim()}` };

  const msg = "ci: add AI-Team label-gated CI lane";
  const commitRes = runGit(repoAbs, ["commit", "-m", msg]);
  if (!commitRes.ok && !commitRes.stderr.toLowerCase().includes("nothing to commit")) {
    return { ok: false, message: `Failed to git commit: ${commitRes.stderr.trim() || commitRes.stdout.trim()}` };
  }

  const sha = runGit(repoAbs, ["rev-parse", "HEAD"]);
  result.commit = sha.ok ? sha.stdout.trim() : null;

  const pushRes = runGit(repoAbs, ["push", "-u", "origin", workBranch]);
  if (!pushRes.ok) return { ok: false, message: `Failed to git push: ${pushRes.stderr.trim() || pushRes.stdout.trim()}` };

  const repoFullName = parseRepoFullNameFromOrigin(repoAbs);
  if (!repoFullName) return { ok: false, message: "Cannot parse GitHub repo full name from origin remote URL; update origin to a github.com URL." };

  const prTitle = "AI-Team: install label-gated CI workflow";
  const prBody = [
    "Installs `.github/workflows/ai-team-ci.yml`.",
    "",
    "This workflow runs only on PRs labeled `ai-team` and never deploys.",
  ].join("\n");

  // Reuse existing PR if any, otherwise create.
  const prList = spawnSync("gh", ["pr", "list", "--repo", repoFullName, "--state", "open", "--head", workBranch, "--json", "number,url", "--limit", "1"], { cwd: repoAbs, encoding: "utf8" });
  let prUrl = null;
  let prNumber = null;
  if (prList.status === 0) {
    try {
      const parsed = JSON.parse(String(prList.stdout || ""));
      const first = Array.isArray(parsed) && parsed.length ? parsed[0] : null;
      if (first && typeof first.url === "string") prUrl = first.url;
      if (first && typeof first.number === "number") prNumber = first.number;
    } catch {
      // ignore and create
    }
  }

  if (!prUrl) {
    const created = createPr({ repo: repoFullName, base: baseBranch, head: workBranch, title: prTitle, body: prBody, labels: ["ai-team"] });
    prUrl = created.url;
    prNumber = prNumberFromUrl(prUrl);
  } else {
    addPrLabel({ repo: repoFullName, prNumber: prNumber || prUrl, labels: ["ai-team"] });
  }

  result.pr = { repo_full_name: repoFullName, pr_number: prNumber, pr_url: prUrl, base_branch: baseBranch, head_branch: workBranch, labels_applied: ["ai-team"], created_at: nowISO() };
  return result;
}
