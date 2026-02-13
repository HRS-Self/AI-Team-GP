function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
}

function normalizeStringArray(arr) {
  if (!Array.isArray(arr)) return null;
  const out = [];
  for (const v of arr) {
    const s = String(v || "").trim();
    if (!s) continue;
    out.push(s);
  }
  return out;
}

function impactWeight(x) {
  if (x === "high") return 0;
  if (x === "medium") return 1;
  if (x === "low") return 2;
  return 99;
}

function gapSortKey(g) {
  const impact = String(g.impact || "").trim();
  const risk = String(g.risk_level || "").trim();
  const id = String(g.gap_id || "").trim();
  return { impactW: impactWeight(impact), riskW: impactWeight(risk), id };
}

function isSortedBy(items, cmp) {
  for (let i = 1; i < items.length; i += 1) {
    if (cmp(items[i - 1], items[i]) > 0) return false;
  }
  return true;
}

export function validateGaps(raw, { teamsById = null, reposById = null, expectedProjectCode = null } = {}) {
  const errors = [];
  const add = (m) => errors.push(String(m));

  if (!isPlainObject(raw)) return { ok: false, errors: ["GAPS.json must be a JSON object."], normalized: null };
  if (raw.version !== 1) add("GAPS.json.version must be 1.");

  const project_code = isNonEmptyString(raw.project_code) ? raw.project_code.trim() : null;
  if (!project_code) add("GAPS.json.project_code must be a non-empty string.");
  if (expectedProjectCode && project_code && project_code !== expectedProjectCode) add(`GAPS.json.project_code mismatch (expected ${expectedProjectCode}).`);

  const baseline = isNonEmptyString(raw.baseline) ? raw.baseline.trim() : null;
  if (!baseline) add("GAPS.json.baseline must be a non-empty string.");

  const generated_at = isNonEmptyString(raw.generated_at) ? raw.generated_at.trim() : null;
  if (!generated_at) add("GAPS.json.generated_at must be a non-empty ISO string.");

  const itemsRaw = Array.isArray(raw.items) ? raw.items : null;
  if (!itemsRaw) add("GAPS.json.items must be an array.");

  const seen = new Set();
  const items = [];

  if (itemsRaw) {
    for (let i = 0; i < itemsRaw.length; i += 1) {
      const g = itemsRaw[i];
      if (!isPlainObject(g)) {
        add(`items[${i}] must be an object.`);
        continue;
      }

      const gap_id = isNonEmptyString(g.gap_id) ? g.gap_id.trim() : null;
      if (!gap_id) add(`items[${i}].gap_id must be a non-empty string.`);
      if (gap_id) {
        if (seen.has(gap_id)) add(`items[${i}].gap_id duplicates '${gap_id}'.`);
        seen.add(gap_id);
      }

      const title = isNonEmptyString(g.title) ? g.title.trim() : null;
      const summary = isNonEmptyString(g.summary) ? g.summary.trim() : null;
      const recommended_action = isNonEmptyString(g.recommended_action) ? g.recommended_action.trim() : null;
      if (!title) add(`items[${i}].title must be a non-empty string.`);
      if (!summary) add(`items[${i}].summary must be a non-empty string.`);
      if (!recommended_action) add(`items[${i}].recommended_action must be a non-empty string.`);

      const observed_evidence = normalizeStringArray(g.observed_evidence);
      if (!observed_evidence) add(`items[${i}].observed_evidence must be an array of strings.`);
      if (observed_evidence && observed_evidence.length === 0) add(`items[${i}].observed_evidence must not be empty.`);

      const impact = isNonEmptyString(g.impact) ? g.impact.trim() : null;
      if (!impact || !["low", "medium", "high"].includes(impact)) add(`items[${i}].impact must be one of: low|medium|high.`);

      const risk_level = isNonEmptyString(g.risk_level) ? g.risk_level.trim() : null;
      if (!risk_level || !["low", "medium", "high"].includes(risk_level)) add(`items[${i}].risk_level must be one of: low|medium|high.`);

      const target_teams = normalizeStringArray(g.target_teams);
      if (!target_teams) add(`items[${i}].target_teams must be an array of team_id strings.`);
      if (target_teams && target_teams.length === 0) add(`items[${i}].target_teams must not be empty.`);
      if (target_teams && teamsById) {
        for (const t of target_teams) if (!teamsById.has(t)) add(`items[${i}].target_teams contains unknown team_id '${t}'.`);
      }

      const target_repos = g.target_repos === null || typeof g.target_repos === "undefined" ? null : normalizeStringArray(g.target_repos);
      if (g.target_repos !== null && typeof g.target_repos !== "undefined" && !target_repos) add(`items[${i}].target_repos must be an array of repo_id strings or null.`);
      if (target_repos && reposById) {
        for (const r of target_repos) if (!reposById.has(r)) add(`items[${i}].target_repos contains unknown repo_id '${r}'.`);
      }

      const acceptance_criteria = normalizeStringArray(g.acceptance_criteria);
      if (!acceptance_criteria) add(`items[${i}].acceptance_criteria must be an array of strings.`);
      if (acceptance_criteria && acceptance_criteria.length === 0) add(`items[${i}].acceptance_criteria must not be empty.`);

      const deps = isPlainObject(g.dependencies) ? g.dependencies : null;
      if (!deps) add(`items[${i}].dependencies must be an object.`);
      const must_run_after = deps ? normalizeStringArray(deps.must_run_after) : null;
      const can_run_in_parallel_with = deps ? normalizeStringArray(deps.can_run_in_parallel_with) : null;
      if (deps && must_run_after === null) add(`items[${i}].dependencies.must_run_after must be an array of strings.`);
      if (deps && can_run_in_parallel_with === null) add(`items[${i}].dependencies.can_run_in_parallel_with must be an array of strings.`);

      const ssot_refs = normalizeStringArray(g.ssot_refs);
      if (!ssot_refs) add(`items[${i}].ssot_refs must be an array of strings (use [] for none).`);

      const confidence = typeof g.confidence === "number" ? g.confidence : NaN;
      if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) add(`items[${i}].confidence must be a number between 0 and 1.`);

      if (
        gap_id &&
        title &&
        summary &&
        recommended_action &&
        observed_evidence &&
        impact &&
        risk_level &&
        target_teams &&
        acceptance_criteria &&
        deps &&
        ssot_refs &&
        Number.isFinite(confidence)
      ) {
        items.push({
          gap_id,
          title,
          summary,
          observed_evidence,
          impact,
          risk_level,
          recommended_action,
          target_teams,
          target_repos: target_repos || null,
          acceptance_criteria,
          dependencies: { must_run_after: must_run_after || [], can_run_in_parallel_with: can_run_in_parallel_with || [] },
          ssot_refs: ssot_refs || [],
          confidence,
        });
      }
    }
  }

  // Enforce stable file ordering requirement.
  if (itemsRaw && Array.isArray(itemsRaw) && itemsRaw.length) {
    const cmp = (a, b) => {
      const ka = gapSortKey(a);
      const kb = gapSortKey(b);
      if (ka.impactW !== kb.impactW) return ka.impactW - kb.impactW;
      if (ka.riskW !== kb.riskW) return ka.riskW - kb.riskW;
      return String(ka.id).localeCompare(String(kb.id));
    };
    if (!isSortedBy(itemsRaw, cmp)) add("GAPS.json.items must be sorted by (impact desc high->low, risk_level desc, gap_id asc).");
  }

  if (errors.length) return { ok: false, errors, normalized: null };
  return { ok: true, errors: [], normalized: { version: 1, project_code, baseline, generated_at, items } };
}
