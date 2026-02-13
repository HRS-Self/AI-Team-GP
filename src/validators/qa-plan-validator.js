function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function containsAbsolutePathLike(text) {
  const s = String(text || "");
  if (!s) return false;
  if (s.includes("/opt/")) return true;
  if (/[A-Za-z]:\\\\/.test(s)) return true;
  if (s.startsWith("/")) return true;
  return false;
}

function scanForAbsolutePaths(value, path, errors) {
  if (typeof value === "string") {
    if (containsAbsolutePathLike(value)) errors.push(`${path}: absolute paths are forbidden in QA artifacts.`);
    return;
  }
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i += 1) scanForAbsolutePaths(value[i], `${path}[${i}]`, errors);
    return;
  }
  if (isPlainObject(value)) {
    for (const k of Object.keys(value)) scanForAbsolutePaths(value[k], `${path}.${k}`, errors);
  }
}

export function validateQaPlan(raw, { expectedWorkId = null, expectedRepoId = null } = {}) {
  const errors = [];
  if (!isPlainObject(raw)) return { ok: false, errors: ["QA plan must be a JSON object."], normalized: null };

  if (raw.version !== 1) errors.push("qa-plan.version must be 1.");
  if (expectedWorkId && String(raw.work_id || "") !== String(expectedWorkId)) errors.push("qa-plan.work_id does not match workId.");
  if (expectedRepoId && String(raw.repo_id || "") !== String(expectedRepoId)) errors.push("qa-plan.repo_id does not match repo_id.");

  const requiredString = (path, v) => {
    if (typeof v !== "string" || !v.trim()) errors.push(`${path} missing/empty.`);
  };
  requiredString("qa-plan.work_id", raw.work_id);
  requiredString("qa-plan.repo_id", raw.repo_id);
  requiredString("qa-plan.team_id", raw.team_id);
  requiredString("qa-plan.target_branch", raw.target_branch);
  requiredString("qa-plan.created_at", raw.created_at);

  if (!Array.isArray(raw.ssot_references)) {
    errors.push("qa-plan.ssot_references must be an array (required; may be empty only if explicitly allowed by policy).");
  } else {
    for (let i = 0; i < raw.ssot_references.length; i += 1) {
      const r = raw.ssot_references[i];
      const doc = typeof r?.doc === "string" ? r.doc.trim() : "";
      const section = typeof r?.section === "string" ? r.section.trim() : "";
      const rule_id = typeof r?.rule_id === "string" ? r.rule_id.trim() : "";
      if (!doc || !section || !rule_id) errors.push(`qa-plan.ssot_references[${i}] must include non-empty {doc, section, rule_id}.`);
    }
    if (raw.ssot_references.length < 1) errors.push("qa-plan.ssot_references must contain at least 1 entry.");
  }

  if (!isPlainObject(raw.ssot)) errors.push("qa-plan.ssot must be an object.");
  else {
    requiredString("qa-plan.ssot.bundle_path", raw.ssot.bundle_path);
    requiredString("qa-plan.ssot.bundle_hash", raw.ssot.bundle_hash);
    requiredString("qa-plan.ssot.snapshot_sha256", raw.ssot.snapshot_sha256);
  }

  if (!isPlainObject(raw.derived_from)) errors.push("qa-plan.derived_from must be an object.");
  else {
    requiredString("qa-plan.derived_from.proposal_path", raw.derived_from.proposal_path);
    requiredString("qa-plan.derived_from.proposal_sha256", raw.derived_from.proposal_sha256);
    requiredString("qa-plan.derived_from.patch_plan_path", raw.derived_from.patch_plan_path);
    requiredString("qa-plan.derived_from.patch_plan_sha256", raw.derived_from.patch_plan_sha256);
    requiredString("qa-plan.derived_from.timestamp", raw.derived_from.timestamp);
  }

  if (!Array.isArray(raw.tests)) errors.push("qa-plan.tests must be an array.");
  if (!Array.isArray(raw.gaps)) errors.push("qa-plan.gaps must be an array.");

  if (Array.isArray(raw.tests)) {
    raw.tests.forEach((t, i) => {
      if (!isPlainObject(t)) {
        errors.push(`qa-plan.tests[${i}] must be an object.`);
        return;
      }
      requiredString(`qa-plan.tests[${i}].test_id`, t.test_id);
      requiredString(`qa-plan.tests[${i}].title`, t.title);
      const type = String(t.type || "").trim();
      if (!["unit", "integration", "e2e", "manual"].includes(type)) errors.push(`qa-plan.tests[${i}].type invalid.`);
      const pri = String(t.priority || "").trim();
      if (!["P0", "P1", "P2", "P3"].includes(pri)) errors.push(`qa-plan.tests[${i}].priority invalid.`);
      if (!Array.isArray(t.acceptance_criteria)) errors.push(`qa-plan.tests[${i}].acceptance_criteria must be an array.`);
      if (!Array.isArray(t.ssot_refs)) errors.push(`qa-plan.tests[${i}].ssot_refs must be an array.`);
    });
  }
  if (Array.isArray(raw.gaps)) {
    raw.gaps.forEach((g, i) => {
      if (!isPlainObject(g)) {
        errors.push(`qa-plan.gaps[${i}] must be an object.`);
        return;
      }
      requiredString(`qa-plan.gaps[${i}].gap_id`, g.gap_id);
      requiredString(`qa-plan.gaps[${i}].description`, g.description);
      const impact = String(g.impact || "").trim();
      if (!["low", "medium", "high"].includes(impact)) errors.push(`qa-plan.gaps[${i}].impact invalid.`);
      if (!Array.isArray(g.ssot_refs)) errors.push(`qa-plan.gaps[${i}].ssot_refs must be an array.`);
    });
  }

  // Absolute paths forbidden anywhere in QA artifact JSON.
  scanForAbsolutePaths(raw, "qa", errors);

  // SSOT citations are mandatory and must include at least one ref across tests/gaps.
  const tests = Array.isArray(raw.tests) ? raw.tests : [];
  const gaps = Array.isArray(raw.gaps) ? raw.gaps : [];
  const hasRefs =
    tests.some((t) => isPlainObject(t) && Array.isArray(t.ssot_refs) && t.ssot_refs.some((r) => String(r || "").trim())) ||
    gaps.some((g) => isPlainObject(g) && Array.isArray(g.ssot_refs) && g.ssot_refs.some((r) => String(r || "").trim()));
  if (!hasRefs) errors.push("qa-plan must include at least one ssot_refs entry in tests[] or gaps[].");

  // Derived-from hashes must be non-empty.
  const df = isPlainObject(raw.derived_from) ? raw.derived_from : null;
  if (df) {
    if (!String(df.proposal_sha256 || "").trim()) errors.push("qa-plan.derived_from.proposal_sha256 missing/empty.");
    if (!String(df.patch_plan_sha256 || "").trim()) errors.push("qa-plan.derived_from.patch_plan_sha256 missing/empty.");
  }

  return { ok: errors.length === 0, errors, normalized: raw };
}
