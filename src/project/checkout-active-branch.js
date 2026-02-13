import { existsSync, readFileSync } from "node:fs";
import { mkdir, readFile, writeFile, appendFile as appendFileNative, readdir } from "node:fs/promises";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";

import { formatFsSafeUtcTimestamp } from "../utils/naming.js";

function nowISO() {
  return new Date().toISOString();
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function jsonStable(obj) {
  return JSON.stringify(obj, null, 2) + "\n";
}

async function pathExists(p) {
  try {
    await readdir(p);
    return true;
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return false;
    return false;
  }
}

function run(cmd, { cwd, dryRun }) {
  if (dryRun) return { ok: true, status: 0, stdout: `(dry-run) ${cmd}\n`, stderr: "" };
  const res = spawnSync(cmd, { cwd, shell: true, encoding: "utf8" });
  return {
    ok: res.status === 0,
    status: typeof res.status === "number" ? res.status : null,
    stdout: String(res.stdout || ""),
    stderr: String(res.stderr || ""),
  };
}

function git(cwd, args, { dryRun }) {
  const cmd = ["git", "-C", cwd, ...args].map((s) => `"${String(s).replaceAll('"', '\\"')}"`).join(" ");
  return run(cmd, { cwd, dryRun });
}

function isGitRepo(cwd) {
  return existsSync(resolve(cwd, ".git"));
}

async function readJson(pathAbs) {
  const text = await readFile(pathAbs, "utf8");
  return JSON.parse(text);
}

function detectRepoFiles(repoPath) {
  const has = (p) => existsSync(resolve(repoPath, p));
  return {
    node: {
      package_json: has("package.json"),
      pnpm_lock: has("pnpm-lock.yaml"),
      yarn_lock: has("yarn.lock"),
      npm_lock: has("package-lock.json") || has("npm-shrinkwrap.json"),
    },
  };
}

function readPackageJsonScripts(repoPath) {
  const pkgPath = resolve(repoPath, "package.json");
  if (!existsSync(pkgPath)) return { ok: true, scripts: {}, package_json: null };
  try {
    const text = readFileSync(pkgPath, "utf8");
    const json = JSON.parse(String(text || ""));
    const scripts = isPlainObject(json?.scripts) ? json.scripts : {};
    return { ok: true, scripts, package_json: pkgPath };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: msg, scripts: {}, package_json: pkgPath };
  }
}

function inferNodeCommands({ files, scripts }) {
  const pm = files.node.pnpm_lock ? "pnpm" : files.node.yarn_lock ? "yarn" : files.node.package_json ? "npm" : null;
  const hasScript = (k) => Object.prototype.hasOwnProperty.call(scripts || {}, k);

  const commands = { cwd: ".", package_manager: pm, install: null, lint: null, test: null, build: null };
  if (!pm) return { ok: true, commands, missing: { node: true } };

  if (pm === "pnpm") commands.install = files.node.pnpm_lock ? "pnpm install --frozen-lockfile" : "pnpm install";
  if (pm === "yarn") commands.install = files.node.yarn_lock ? "yarn install --frozen-lockfile" : "yarn install";
  if (pm === "npm") commands.install = files.node.npm_lock ? "npm ci" : "npm install";

  for (const k of ["lint", "test", "build"]) {
    if (!hasScript(k)) continue;
    commands[k] = pm === "yarn" ? `yarn ${k}` : pm === "pnpm" ? `pnpm ${k}` : `npm run ${k}`;
  }

  return { ok: true, commands, missing: { lint: !hasScript("lint"), test: !hasScript("test"), build: !hasScript("build") } };
}

async function appendDecisionNeeded({ projectRoot, workId, repoId, canonicalPath, requestedBranch, reason, branches }) {
  const decisionsPath = resolve(projectRoot, "ai", "DECISIONS_NEEDED.md");
  let existing = "";
  try {
    existing = await readFile(decisionsPath, "utf8");
  } catch (err) {
    if (!err || (err.code !== "ENOENT" && err.code !== "ENOTDIR")) throw err;
  }

  const marker = `CheckoutActiveBranchDecision:${workId}:${repoId}:${reason}`;
  if (existing.includes(marker)) return;

  const lines = [];
  lines.push("");
  lines.push(`## Canonical branch checkout decision required (${workId})`);
  lines.push("");
  lines.push(`- repo_id: \`${repoId}\``);
  lines.push(`- canonical_path: \`${canonicalPath}\``);
  lines.push(`- requested_branch: \`${requestedBranch || "(none)"}\``);
  lines.push(`- reason: \`${reason}\``);
  if (Array.isArray(branches) && branches.length) {
    lines.push("");
    lines.push("Remote branches (origin):");
    lines.push("");
    for (const b of branches.slice(0, 40)) lines.push(`- \`${b}\``);
    if (branches.length > 40) lines.push(`- ... (${branches.length - 40} more)`);
  }
  lines.push("");
  lines.push("Next action:");
  lines.push("");
  lines.push("- Confirm the correct branch and set `repos[].active_branch` in `config/REPOS.json`, then rerun `--checkout-active-branch`.");
  lines.push("");
  lines.push(`<!-- ${marker} -->`);
  lines.push("");

  await mkdir(resolve(projectRoot, "ai"), { recursive: true });
  await appendFileNative(decisionsPath, existing.trimEnd() + "\n" + lines.join("\n"), "utf8");
}

function listOriginRemoteBranches(repoPath, { dryRun }) {
  const res = git(repoPath, ["for-each-ref", "refs/remotes/origin", "--format=%(refname:short)"], { dryRun });
  if (!res.ok) return [];
  return res.stdout
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .map((r) => (r.startsWith("origin/") ? r.slice("origin/".length) : r))
    .filter((b) => b !== "HEAD")
    .sort((a, b) => a.localeCompare(b));
}

function originHasBranch(repoPath, branchName, { dryRun }) {
  const b = String(branchName || "").trim();
  if (!b) return false;
  const res = git(repoPath, ["show-ref", "--verify", "--quiet", `refs/remotes/origin/${b}`], { dryRun });
  return !!res.ok;
}

function headInfo(repoPath, { dryRun }) {
  const sha = git(repoPath, ["rev-parse", "HEAD"], { dryRun });
  const br = git(repoPath, ["rev-parse", "--abbrev-ref", "HEAD"], { dryRun });
  return {
    sha: sha.ok ? sha.stdout.trim() : null,
    branch: br.ok ? br.stdout.trim() : null,
  };
}

function resolveTargetBranch({ repo, repoPath, dryRun }) {
  const requested = typeof repo?.active_branch === "string" ? repo.active_branch.trim() : "";
  if (requested) return { requested, resolved: requested, source: "active_branch" };

  const candidates = ["develop", "main", "master"];
  for (const c of candidates) {
    if (originHasBranch(repoPath, c, { dryRun })) return { requested: null, resolved: c, source: "fallback" };
  }
  return { requested: null, resolved: null, source: "none" };
}

async function resolveProjectRootFromArgs({ workRoot }) {
  const explicit = typeof workRoot === "string" && workRoot.trim() ? resolve(workRoot.trim()) : null;
  if (explicit) return explicit;
  const cwd = process.cwd();
  if (existsSync(resolve(cwd, "ai", "REPOS.json"))) return cwd;
  return null;
}

export async function runCheckoutActiveBranch({
  workRoot,
  dryRun = false,
  limit = null,
  repoId = null,
  onlyActive = false,
  rescanCommands = false,
}) {
  const projectRoot = await resolveProjectRootFromArgs({ workRoot });
  if (!projectRoot) {
    return {
      ok: false,
      message: "Cannot determine project root. Provide --workRoot or run from a directory containing config/REPOS.json.",
    };
  }

  const reposJsonPath = resolve(projectRoot, "ai", "REPOS.json");
  const logsDir = resolve(projectRoot, "ai", "logs");
  const runId = formatFsSafeUtcTimestamp(new Date());
  const artifactsDir = resolve(projectRoot, "ai", "artifacts", "checkout-active-branch", runId);
  await mkdir(logsDir, { recursive: true });
  await mkdir(artifactsDir, { recursive: true });

  let reposDoc;
  try {
    reposDoc = await readJson(reposJsonPath);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Failed to read project config/REPOS.json (${msg}).`, projectRoot, reposJsonPath };
  }

  if (reposDoc?.version !== 1 || typeof reposDoc?.base_dir !== "string" || !Array.isArray(reposDoc?.repos)) {
    return { ok: false, message: "Invalid config/REPOS.json structure (expected {version:1, base_dir, repos[]}).", projectRoot, reposJsonPath };
  }

  const baseDir = resolve(String(reposDoc.base_dir));
  let repos = reposDoc.repos.slice().filter((r) => isPlainObject(r) && typeof r.repo_id === "string");

  if (onlyActive) {
    repos = repos.filter((r) => String(r?.status || "").trim().toLowerCase() === "active");
  }
  if (repoId) {
    repos = repos.filter((r) => String(r.repo_id).trim() === repoId);
  }

  repos.sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)));
  if (Number.isFinite(limit) && limit > 0) repos = repos.slice(0, limit);

  const stdoutLines = [];
  const stderrLines = [];

  function logOut(line) {
    stdoutLines.push(String(line));
  }
  function logErr(line) {
    stderrLines.push(String(line));
  }

  const started = nowISO();
  const ledgerPath = resolve(projectRoot, "ai", "ledger.jsonl");
  const workId = `checkout-active-branch:${runId}`;
  await appendFileNative(
    ledgerPath,
    JSON.stringify({
      timestamp: started,
      action: rescanCommands ? "canonical_commands_rescan_started" : "canonical_branch_checkout_started",
      work_id: workId,
    }) + "\n",
    "utf8",
  );

  const results = [];
  let failedCount = 0;

  for (const repo of repos) {
    const rid = String(repo.repo_id).trim();
    const repoPathRel = String(repo.path || "").trim();
    const canonicalPath = resolve(baseDir, repoPathRel);

    const before = headInfo(canonicalPath, { dryRun });
    const requestedBranch = typeof repo?.active_branch === "string" ? repo.active_branch.trim() : "";

    const res = {
      repo_id: rid,
      path: repoPathRel,
      canonical_path: canonicalPath,
      requested_branch: requestedBranch || null,
      resolved_branch: null,
      branch_source: null,
      success: false,
      failure_reason: null,
      git_head_before: before.sha,
      git_head_after: null,
      git_branch_before: before.branch,
      git_branch_after: null,
      notes: [],
      stdout_snippet: "",
      stderr_snippet: "",
    };

    logOut(`== ${rid} ==`);
    if (!(await pathExists(canonicalPath))) {
      res.failure_reason = "missing_dir";
      failedCount += 1;
      results.push(res);
      await appendFileNative(
        ledgerPath,
        JSON.stringify({ timestamp: nowISO(), action: "canonical_branch_checkout_repo_result", work_id: workId, repo_id: rid, ok: false, reason: res.failure_reason }) + "\n",
        "utf8",
      );
      continue;
    }
    if (!isGitRepo(canonicalPath)) {
      res.failure_reason = "not_git_repo";
      failedCount += 1;
      results.push(res);
      await appendFileNative(
        ledgerPath,
        JSON.stringify({ timestamp: nowISO(), action: "canonical_branch_checkout_repo_result", work_id: workId, repo_id: rid, ok: false, reason: res.failure_reason }) + "\n",
        "utf8",
      );
      continue;
    }

    if (rescanCommands) {
      const files = detectRepoFiles(canonicalPath);
      const scriptsRes = files.node.package_json ? readPackageJsonScripts(canonicalPath) : { ok: true, scripts: {}, package_json: null };
      if (!scriptsRes.ok) {
        res.failure_reason = "invalid_package_json";
        res.stderr_snippet = String(scriptsRes.message || "").slice(0, 500);
        failedCount += 1;
        results.push(res);
        await appendFileNative(
          ledgerPath,
          JSON.stringify({
            timestamp: nowISO(),
            action: "canonical_commands_rescan_repo_result",
            work_id: workId,
            repo_id: rid,
            ok: false,
            reason: res.failure_reason,
          }) + "\n",
          "utf8",
        );
        continue;
      }

      const inferred = inferNodeCommands({ files, scripts: scriptsRes.scripts });
      if (!inferred.commands.package_manager) {
        res.failure_reason = "no_package_json";
        failedCount += 1;
        results.push(res);
        await appendFileNative(
          ledgerPath,
          JSON.stringify({
            timestamp: nowISO(),
            action: "canonical_commands_rescan_repo_result",
            work_id: workId,
            repo_id: rid,
            ok: false,
            reason: res.failure_reason,
          }) + "\n",
          "utf8",
        );
        continue;
      }

      // Update in-memory registry entry.
      res.success = true;
      res.notes.push(`commands detected (package_manager=${inferred.commands.package_manager})`);
      repo.commands = {
        cwd: inferred.commands.cwd,
        package_manager: inferred.commands.package_manager,
        install: inferred.commands.install,
        lint: inferred.commands.lint,
        test: inferred.commands.test,
        build: inferred.commands.build,
      };
      results.push(res);

      await appendFileNative(
        ledgerPath,
        JSON.stringify({
          timestamp: nowISO(),
          action: "canonical_commands_rescan_repo_result",
          work_id: workId,
          repo_id: rid,
          ok: true,
          package_manager: inferred.commands.package_manager,
        }) + "\n",
        "utf8",
      );
      continue;
    }

    const remoteV = git(canonicalPath, ["remote", "-v"], { dryRun });
    if (!remoteV.ok || !remoteV.stdout.includes("origin")) {
      res.failure_reason = "origin_missing";
      res.stderr_snippet = (remoteV.stderr || remoteV.stdout || "").slice(0, 500);
      failedCount += 1;
      results.push(res);
      await appendFileNative(
        ledgerPath,
        JSON.stringify({ timestamp: nowISO(), action: "canonical_branch_checkout_repo_result", work_id: workId, repo_id: rid, ok: false, reason: res.failure_reason }) + "\n",
        "utf8",
      );
      continue;
    }

    logOut(`repo=${canonicalPath}`);
    if (dryRun) logOut("(dry-run) skipping git operations");

    const fetchRes = git(canonicalPath, ["fetch", "--prune", "origin"], { dryRun });
    if (!fetchRes.ok) {
      res.failure_reason = "fetch_failed";
      res.stderr_snippet = (fetchRes.stderr || "").slice(0, 800);
      failedCount += 1;
      results.push(res);
      await appendFileNative(
        ledgerPath,
        JSON.stringify({ timestamp: nowISO(), action: "canonical_branch_checkout_repo_result", work_id: workId, repo_id: rid, ok: false, reason: res.failure_reason }) + "\n",
        "utf8",
      );
      continue;
    }

    const resolved = resolveTargetBranch({ repo, repoPath: canonicalPath, dryRun });
    res.branch_source = resolved.source;
    res.resolved_branch = resolved.resolved;

    if (!resolved.resolved) {
      res.failure_reason = "no_default_branch";
      const branches = listOriginRemoteBranches(canonicalPath, { dryRun });
      await appendDecisionNeeded({
        projectRoot,
        workId,
        repoId: rid,
        canonicalPath,
        requestedBranch: requestedBranch || null,
        reason: "no_default_branch",
        branches,
      });
      failedCount += 1;
      results.push(res);
      await appendFileNative(
        ledgerPath,
        JSON.stringify({ timestamp: nowISO(), action: "canonical_branch_checkout_repo_result", work_id: workId, repo_id: rid, ok: false, reason: res.failure_reason }) + "\n",
        "utf8",
      );
      continue;
    }

    if (!originHasBranch(canonicalPath, resolved.resolved, { dryRun })) {
      res.failure_reason = "remote_branch_missing";
      const branches = listOriginRemoteBranches(canonicalPath, { dryRun });
      await appendDecisionNeeded({
        projectRoot,
        workId,
        repoId: rid,
        canonicalPath,
        requestedBranch: resolved.resolved,
        reason: "remote_branch_missing",
        branches,
      });
      failedCount += 1;
      results.push(res);
      await appendFileNative(
        ledgerPath,
        JSON.stringify({ timestamp: nowISO(), action: "canonical_branch_checkout_repo_result", work_id: workId, repo_id: rid, ok: false, reason: res.failure_reason, branch: resolved.resolved }) + "\n",
        "utf8",
      );
      continue;
    }

    const sw = git(canonicalPath, ["switch", resolved.resolved], { dryRun });
    if (!sw.ok) {
      // Allowed: create local tracking branch for origin/<branch>
      const sw2 = git(canonicalPath, ["switch", "-c", resolved.resolved, "--track", `origin/${resolved.resolved}`], { dryRun });
      if (!sw2.ok) {
        res.failure_reason = "switch_failed";
        res.stderr_snippet = ((sw.stderr || "") + "\n" + (sw2.stderr || "")).slice(0, 800);
        failedCount += 1;
        results.push(res);
        await appendFileNative(
          ledgerPath,
          JSON.stringify({ timestamp: nowISO(), action: "canonical_branch_checkout_repo_result", work_id: workId, repo_id: rid, ok: false, reason: res.failure_reason, branch: resolved.resolved }) + "\n",
          "utf8",
        );
        continue;
      }
    }

    const resetRes = git(canonicalPath, ["reset", "--hard", `origin/${resolved.resolved}`], { dryRun });
    if (!resetRes.ok) {
      res.failure_reason = "reset_failed";
      res.stderr_snippet = (resetRes.stderr || "").slice(0, 800);
      failedCount += 1;
      results.push(res);
      await appendFileNative(
        ledgerPath,
        JSON.stringify({ timestamp: nowISO(), action: "canonical_branch_checkout_repo_result", work_id: workId, repo_id: rid, ok: false, reason: res.failure_reason, branch: resolved.resolved }) + "\n",
        "utf8",
      );
      continue;
    }

    const status = git(canonicalPath, ["status", "--porcelain"], { dryRun });
    const statusText = status.ok ? status.stdout.trim() : "";
    if (status.ok && statusText) {
      res.failure_reason = "dirty_after_reset";
      res.stderr_snippet = statusText.slice(0, 800);
      failedCount += 1;
      results.push(res);
      await appendFileNative(
        ledgerPath,
        JSON.stringify({ timestamp: nowISO(), action: "canonical_branch_checkout_repo_result", work_id: workId, repo_id: rid, ok: false, reason: res.failure_reason, branch: resolved.resolved }) + "\n",
        "utf8",
      );
      continue;
    }

    const after = headInfo(canonicalPath, { dryRun });
    res.git_head_after = after.sha;
    res.git_branch_after = after.branch;
    res.success = true;

    results.push(res);
    await appendFileNative(
      ledgerPath,
      JSON.stringify({
        timestamp: nowISO(),
        action: "canonical_branch_checkout_repo_result",
        work_id: workId,
        repo_id: rid,
        ok: true,
        branch: resolved.resolved,
        head_before: before.sha,
        head_after: after.sha,
      }) + "\n",
      "utf8",
    );
  }

  const finished = nowISO();
  await appendFileNative(
    ledgerPath,
    JSON.stringify({
      timestamp: finished,
      action: rescanCommands ? "canonical_commands_rescan_finished" : "canonical_branch_checkout_finished",
      work_id: workId,
      failed: failedCount,
      total: results.length,
    }) + "\n",
    "utf8",
  );

  const report = {
    version: 1,
    run_id: runId,
    created_at: started,
    project_root: projectRoot,
    repos_json_path: reposJsonPath,
    base_dir: baseDir,
    only_active: !!onlyActive,
    limit: Number.isFinite(limit) ? limit : null,
    repo_filter: repoId || null,
    dry_run: !!dryRun,
    totals: {
      total: results.length,
      ok: results.filter((r) => r.success).length,
      failed: failedCount,
    },
    repos: results,
  };

  await writeFile(resolve(artifactsDir, "report.json"), JSON.stringify(report, null, 2) + "\n", "utf8");

  const md = [];
  md.push(rescanCommands ? "# Rescan Commands Report" : "# Checkout Active Branch Report");
  md.push("");
  md.push(`- created_at: \`${started}\``);
  md.push(`- project_root: \`${projectRoot}\``);
  md.push(`- repos_json: \`${reposJsonPath}\``);
  md.push(`- base_dir: \`${baseDir}\``);
  md.push(`- dry_run: \`${String(!!dryRun)}\``);
  md.push("");
  md.push(`Totals: ok=${report.totals.ok} failed=${report.totals.failed} total=${report.totals.total}`);
  md.push("");
  md.push("| repo_id | branch (requested→resolved) | result | reason |");
  md.push("|---|---|---|---|");
  for (const r of results) {
    const br = `${r.requested_branch || "(none)"}→${r.resolved_branch || "(none)"}`;
    const result = r.success ? "OK" : "FAIL";
    const reason = r.success ? "" : r.failure_reason || "";
    md.push(`| \`${r.repo_id}\` | \`${br}\` | ${result} | ${reason} |`);
  }
  md.push("");
  md.push("Notes:");
  md.push("");
  md.push("- This command updates only canonical clones under `base_dir` via fetch/switch/reset and writes artifacts under `ai/artifacts/checkout-active-branch/`.");
  md.push("- It will not guess branches beyond develop/main/master when `active_branch` is missing.");
  await writeFile(resolve(artifactsDir, "report.md"), md.join("\n") + "\n", "utf8");

  await writeFile(resolve(logsDir, `checkout-active-branch.${runId}.out.log`), stdoutLines.join("\n") + "\n", "utf8");
  await writeFile(resolve(logsDir, `checkout-active-branch.${runId}.err.log`), stderrLines.join("\n") + "\n", "utf8");

  // If this was a rescan run, persist updated commands into project config/REPOS.json (unless dry-run).
  if (rescanCommands) {
    const updated = {
      ...reposDoc,
      repos: reposDoc.repos.map((r) => {
        const rid = String(r?.repo_id || "").trim();
        const changed = repos.find((x) => String(x?.repo_id || "").trim() === rid);
        if (!changed) return r;
        // Only update if scan succeeded and commands are present.
        if (!isPlainObject(changed.commands)) return r;
        return { ...r, commands: changed.commands };
      }),
    };

    if (!dryRun) {
      await writeFile(reposJsonPath, jsonStable(updated), "utf8");
    }
  }

  return {
    ok: failedCount === 0,
    message:
      failedCount === 0
        ? rescanCommands
          ? "Commands rescanned and REPOS.json updated."
          : "Canonical clones normalized."
        : `Some repos failed (${failedCount}/${results.length}). See report: ${resolve(artifactsDir, "report.md")}`,
    projectRoot,
    reposJsonPath,
    artifactsDir,
    totals: report.totals,
    failed_repos: results.filter((r) => !r.success).map((r) => r.repo_id),
  };
}
