function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

/**
 * Classify how apply should behave on rerun for a repo, based on status.json.
 *
 * This is intentionally conservative:
 * - Only resumes post-push/PR steps for specific failure reasons.
 * - Never assumes a prior commit matches a new/updated patch plan.
 */
export function classifyApplyResume({ statusEntry, expectedBranch, currentBundleHash = "" }) {
  const entry = isPlainObject(statusEntry) ? statusEntry : null;
  const expected = String(expectedBranch || "").trim();

  if (!entry) return { mode: "full", reason: "no_prior_status" };

  const priorBundleHash = typeof entry.bundle_hash === "string" ? entry.bundle_hash.trim() : "";
  const current = String(currentBundleHash || "").trim();
  if (current && priorBundleHash && current !== priorBundleHash) {
    return { mode: "full", reason: "bundle_hash_mismatch", details: { current_bundle_hash: currentBundleHash, prior_bundle_hash: priorBundleHash } };
  }

  const status = String(entry.status || "").trim();
  if (status === "succeeded") return { mode: "skip", reason: "already_succeeded" };

  const branch = typeof entry.branch === "string" ? entry.branch.trim() : "";
  const commit = typeof entry.commit === "string" ? entry.commit.trim() : "";
  const reason = typeof entry.reason_code === "string" ? entry.reason_code.trim() : "";

  // If we have an expected deterministic branch name, do not resume across branch mismatch.
  if (expected && branch && branch !== expected) {
    return { mode: "invalid", reason: "branch_mismatch", details: { expected_branch: expected, found_branch: branch } };
  }

  // Resume only the PR step(s) if the patch was committed to the intended branch.
  const resumableReasons = new Set(["pr_create_failed", "pr_label_failed"]);
  if (status === "failed_final" && resumableReasons.has(reason) && branch && commit) {
    return { mode: "resume_pr", reason };
  }

  return { mode: "full", reason: "not_resumable" };
}
