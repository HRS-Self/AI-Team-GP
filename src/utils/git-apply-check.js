import { spawnSync } from "node:child_process";

function runGit({ cwd, args }) {
  const res = spawnSync("git", Array.isArray(args) ? args : [], {
    cwd,
    encoding: "utf8",
  });
  return {
    ok: res.status === 0,
    status: typeof res.status === "number" ? res.status : null,
    stdout: String(res.stdout || ""),
    stderr: String(res.stderr || ""),
  };
}

/**
 * Deterministically classify whether a patch applies, is already applied, or fails.
 *
 * - No inference: uses git's own patch engine.
 * - "already_applied" is determined by `git apply --reverse --check`.
 */
export function classifyGitApplyCheck({ cwd, patchFileAbs, recount = false } = {}) {
  const patch = String(patchFileAbs || "").trim();
  if (!patch) return { ok: false, message: "Missing patchFileAbs." };
  const base = ["apply", ...(recount ? ["--recount"] : []), "--check", patch];
  const forward = runGit({ cwd, args: base });
  if (forward.ok) return { ok: true, status: "applies", forward };

  const reverse = runGit({ cwd, args: ["apply", ...(recount ? ["--recount"] : []), "--reverse", "--check", patch] });
  if (reverse.ok) return { ok: true, status: "already_applied", forward, reverse };

  return { ok: true, status: "fails", forward, reverse };
}

