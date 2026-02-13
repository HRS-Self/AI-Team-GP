import { spawnSync } from "node:child_process";

function git(repoAbs, args) {
  const res = spawnSync("git", ["-C", repoAbs, ...args], { encoding: "utf8" });
  return {
    ok: res.status === 0,
    status: typeof res.status === "number" ? res.status : null,
    stdout: String(res.stdout || "").trim(),
    stderr: String(res.stderr || "").trim(),
  };
}

export function currentBranchName(repoAbs) {
  const res = git(repoAbs, ["rev-parse", "--abbrev-ref", "HEAD"]);
  if (!res.ok) return null;
  const b = String(res.stdout || "").trim();
  return b || null;
}

export function workingTreeIsClean(repoAbs) {
  const res = git(repoAbs, ["status", "--porcelain"]);
  if (!res.ok) return null;
  return res.stdout.trim().length === 0;
}

function resolveBranchRef(repoAbs, branchName) {
  const n = String(branchName || "").trim();
  if (!n) return null;
  if (git(repoAbs, ["show-ref", "--verify", "--quiet", `refs/heads/${n}`]).ok) return { kind: "local", ref: n };
  if (git(repoAbs, ["show-ref", "--verify", "--quiet", `refs/remotes/origin/${n}`]).ok) return { kind: "origin", ref: `origin/${n}` };
  // Fallback: any remote that has /<branchName>
  const remotes = git(repoAbs, ["for-each-ref", "refs/remotes", "--format=%(refname:short)"]);
  if (!remotes.ok) return null;
  const match = remotes.stdout
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .filter((r) => r.endsWith(`/${n}`))
    .sort((a, b) => a.localeCompare(b))[0];
  if (match) return { kind: "remote", ref: match };
  return null;
}

export function checkoutBranchDeterministic(repoAbs, branchName) {
  const to = String(branchName || "").trim();
  if (!to) return { ok: false, reason: "missing_branch" };

  const from = currentBranchName(repoAbs);
  if (!from) return { ok: false, reason: "cannot_read_current_branch" };
  if (from === to) return { ok: true, changed: false, from, to, reason: "already_on_branch" };
  if (from === "HEAD") return { ok: false, reason: "detached_head" };

  const clean = workingTreeIsClean(repoAbs);
  if (clean === false) return { ok: false, reason: "dirty_worktree" };
  if (clean === null) return { ok: false, reason: "cannot_check_worktree" };

  const ref = resolveBranchRef(repoAbs, to);
  if (!ref) return { ok: false, reason: "branch_not_found" };

  // Deterministic checkout:
  // - local: checkout branch
  // - origin/remote: create/update local branch from ref
  const cmdArgs =
    ref.kind === "local"
      ? ["checkout", to]
      : ["checkout", "-B", to, ref.ref];

  const res = git(repoAbs, cmdArgs);
  if (!res.ok) return { ok: false, reason: "checkout_failed", details: res.stderr || res.stdout || "" };

  const now = currentBranchName(repoAbs);
  if (now !== to) return { ok: false, reason: "checkout_unverified", details: `expected=${to} actual=${now || "(null)"}` };

  return { ok: true, changed: true, from, to, reason: `checked_out_${ref.kind}` };
}

