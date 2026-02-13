import { spawnSync } from "node:child_process";

function runGit(repoAbs, args) {
  // Use a per-invocation safe.directory override so read-only git operations work even when the
  // repo is owned by a different OS user (common in service deployments).
  const safe = String(repoAbs || "").trim();
  const safeArgs = safe ? ["-c", `safe.directory=${safe}`] : [];
  const res = spawnSync("git", [...safeArgs, "-C", repoAbs, ...args], { encoding: "utf8" });
  return {
    ok: res.status === 0,
    status: typeof res.status === "number" ? res.status : null,
    stdout: String(res.stdout || ""),
    stderr: String(res.stderr || ""),
  };
}

export function resolveGitRefForBranch(repoAbs, branchName) {
  const b = String(branchName || "").trim();
  if (!b) return null;

  // Prefer origin/<branch> if it exists (matches apply runner behavior).
  const originRef = `refs/remotes/origin/${b}`;
  const origin = runGit(repoAbs, ["show-ref", "--verify", "--quiet", originRef]);
  if (origin.ok) return `origin/${b}`;

  const localRef = `refs/heads/${b}`;
  const local = runGit(repoAbs, ["show-ref", "--verify", "--quiet", localRef]);
  if (local.ok) return b;

  return null;
}

export function gitShowFileAtRef(repoAbs, gitRef, repoRelativePath) {
  const ref = String(gitRef || "").trim();
  const p = String(repoRelativePath || "").trim().replace(/^\/+/, "");
  if (!ref || !p) return { ok: false, content: null, error: "missing_ref_or_path" };

  const res = runGit(repoAbs, ["show", `${ref}:${p}`]);
  if (!res.ok) return { ok: false, content: null, error: (res.stderr || res.stdout || "").trim() || "git_show_failed" };
  return { ok: true, content: res.stdout, error: null };
}

export function headLines(text, n = 40) {
  const count = Math.max(1, Number.isFinite(n) ? Number(n) : 40);
  return String(text || "")
    .split("\n")
    .slice(0, count)
    .join("\n")
    .trimEnd();
}
