import { spawnSync } from "node:child_process";
import { existsSync, statSync } from "node:fs";
import { join } from "node:path";

function normCwd(cwd) {
  return String(cwd || "").trim();
}

export function runGit({ cwd, args, label = null } = {}) {
  const repoCwd = normCwd(cwd);
  const argv = Array.isArray(args) ? args.map((a) => String(a)) : [];
  const res = spawnSync("git", argv, { cwd: repoCwd, encoding: "utf8" });
  const errMsg = res.error instanceof Error ? res.error.message : res.error ? String(res.error) : null;
  return {
    ok: res.status === 0,
    status: typeof res.status === "number" ? res.status : null,
    stdout: String(res.stdout || ""),
    stderr: String(res.stderr || ""),
    error: errMsg,
    cwd: repoCwd,
    args: argv,
    label: label ? String(label) : null,
  };
}

export function gitDotGitInfo(cwd) {
  const repoCwd = normCwd(cwd);
  const p = join(repoCwd, ".git");
  if (!repoCwd) return { exists: false, kind: null, path: p };
  if (!existsSync(p)) return { exists: false, kind: null, path: p };
  try {
    const st = statSync(p);
    if (st.isDirectory()) return { exists: true, kind: "directory", path: p };
    if (st.isFile()) return { exists: true, kind: "file", path: p };
    return { exists: true, kind: "other", path: p };
  } catch {
    return { exists: true, kind: "unknown", path: p };
  }
}

export function probeGitWorkTree({ cwd } = {}) {
  const repoCwd = normCwd(cwd);
  if (!repoCwd) {
    return { ok: false, is_inside_work_tree: false, message: "Missing cwd for git probe.", cwd: repoCwd, dotgit: gitDotGitInfo(repoCwd), git: null };
  }
  if (!existsSync(repoCwd)) {
    return { ok: false, is_inside_work_tree: false, message: `Path does not exist: ${repoCwd}`, cwd: repoCwd, dotgit: gitDotGitInfo(repoCwd), git: null };
  }

  const res = runGit({ cwd: repoCwd, args: ["rev-parse", "--is-inside-work-tree"], label: "git rev-parse --is-inside-work-tree" });
  const inside = res.ok && res.stdout.trim() === "true";
  if (!inside) {
    const stderr = String(res.stderr || "").trim();
    const stdout = String(res.stdout || "").trim();
    const combined = `${stdout}\n${stderr}`.toLowerCase();
    if (combined.includes("detected dubious ownership")) {
      const msg = [
        "Git refused to operate due to 'detected dubious ownership' (safe.directory).",
        `cwd: ${repoCwd}`,
        "Remediation:",
        `  git config --global --add safe.directory ${repoCwd}`,
        `.git: ${gitDotGitInfo(repoCwd).exists ? gitDotGitInfo(repoCwd).kind : "(missing)"}`,
        `stdout: ${JSON.stringify(stdout)}`,
        `stderr: ${JSON.stringify(stderr || res.error || "")}`,
      ].join("\n");
      return { ok: false, is_inside_work_tree: false, message: msg, cwd: repoCwd, dotgit: gitDotGitInfo(repoCwd), git: res, code: "dubious_ownership" };
    }
    const msg = [
      "Not a git work tree (git rev-parse failed).",
      `cwd: ${repoCwd}`,
      `.git: ${gitDotGitInfo(repoCwd).exists ? gitDotGitInfo(repoCwd).kind : "(missing)"}`,
      `stdout: ${JSON.stringify(stdout)}`,
      `stderr: ${JSON.stringify(stderr || res.error || "")}`,
    ].join("\n");
    return { ok: false, is_inside_work_tree: false, message: msg, cwd: repoCwd, dotgit: gitDotGitInfo(repoCwd), git: res };
  }

  return { ok: true, is_inside_work_tree: true, message: null, cwd: repoCwd, dotgit: gitDotGitInfo(repoCwd), git: res };
}

export function getOriginUrl({ cwd } = {}) {
  const repoCwd = normCwd(cwd);
  const res = runGit({ cwd: repoCwd, args: ["remote", "get-url", "origin"], label: "git remote get-url origin" });
  const url = res.ok ? res.stdout.trim() : null;
  if (!res.ok || !url) {
    return {
      ok: false,
      url: null,
      warning: {
        code: "missing_origin",
        message: [
          "Knowledge repo has no origin remote (push skipped).",
          `cwd: ${repoCwd}`,
          `stdout: ${JSON.stringify(res.stdout.trim())}`,
          `stderr: ${JSON.stringify(res.stderr.trim() || res.error || "")}`,
        ].join("\n"),
      },
      git: res,
    };
  }
  return { ok: true, url, warning: null, git: res };
}
