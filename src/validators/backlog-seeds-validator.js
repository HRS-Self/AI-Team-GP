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

function priorityWeight(p) {
  if (p === "P0") return 0;
  if (p === "P1") return 1;
  if (p === "P2") return 2;
  if (p === "P3") return 3;
  return 99;
}

function seedSortKey(s) {
  const phase = Number.isFinite(Number(s.phase)) ? Number(s.phase) : 0;
  const pr = String(s.priority || "").trim();
  const id = String(s.seed_id || "").trim();
  return { phase, prWeight: priorityWeight(pr), id };
}

function isSortedBy(items, cmp) {
  for (let i = 1; i < items.length; i += 1) {
    if (cmp(items[i - 1], items[i]) > 0) return false;
  }
  return true;
}

export function validateBacklogSeeds(raw, { teamsById = null, reposById = null, expectedProjectCode = null } = {}) {
  const errors = [];
  const add = (m) => errors.push(String(m));

  if (!isPlainObject(raw)) return { ok: false, errors: ["BACKLOG_SEEDS.json must be a JSON object."], normalized: null };
  if (raw.version !== 1) add("BACKLOG_SEEDS.json.version must be 1.");

  const project_code = isNonEmptyString(raw.project_code) ? raw.project_code.trim() : null;
  if (!project_code) add("BACKLOG_SEEDS.json.project_code must be a non-empty string.");
  if (expectedProjectCode && project_code && project_code !== expectedProjectCode) add(`BACKLOG_SEEDS.json.project_code mismatch (expected ${expectedProjectCode}).`);

  const generated_at = isNonEmptyString(raw.generated_at) ? raw.generated_at.trim() : null;
  if (!generated_at) add("BACKLOG_SEEDS.json.generated_at must be a non-empty ISO string.");

  const itemsRaw = Array.isArray(raw.items) ? raw.items : null;
  if (!itemsRaw) add("BACKLOG_SEEDS.json.items must be an array.");

  const seen = new Set();
  const items = [];

  if (itemsRaw) {
    for (let i = 0; i < itemsRaw.length; i += 1) {
      const s = itemsRaw[i];
      if (!isPlainObject(s)) {
        add(`items[${i}] must be an object.`);
        continue;
      }

      const seed_id = isNonEmptyString(s.seed_id) ? s.seed_id.trim() : null;
      if (!seed_id) add(`items[${i}].seed_id must be a non-empty string.`);
      if (seed_id) {
        if (seen.has(seed_id)) add(`items[${i}].seed_id duplicates '${seed_id}'.`);
        seen.add(seed_id);
      }

      const title = isNonEmptyString(s.title) ? s.title.trim() : null;
      const summary = isNonEmptyString(s.summary) ? s.summary.trim() : null;
      const rationale = isNonEmptyString(s.rationale) ? s.rationale.trim() : null;
      if (!title) add(`items[${i}].title must be a non-empty string.`);
      if (!summary) add(`items[${i}].summary must be a non-empty string.`);
      if (!rationale) add(`items[${i}].rationale must be a non-empty string.`);

      const phase = Number.isFinite(Number(s.phase)) ? Number(s.phase) : NaN;
      if (!Number.isInteger(phase) || phase < 1) add(`items[${i}].phase must be an integer >= 1.`);

      const priority = isNonEmptyString(s.priority) ? s.priority.trim() : null;
      if (!priority || !["P0", "P1", "P2", "P3"].includes(priority)) add(`items[${i}].priority must be one of: P0|P1|P2|P3.`);

      const target_teams = normalizeStringArray(s.target_teams);
      if (!target_teams) add(`items[${i}].target_teams must be an array of team_id strings.`);
      if (target_teams && target_teams.length === 0) add(`items[${i}].target_teams must not be empty.`);
      if (target_teams && teamsById) {
        for (const t of target_teams) if (!teamsById.has(t)) add(`items[${i}].target_teams contains unknown team_id '${t}'.`);
      }

      const target_repos = s.target_repos === null || typeof s.target_repos === "undefined" ? null : normalizeStringArray(s.target_repos);
      if (s.target_repos !== null && typeof s.target_repos !== "undefined" && !target_repos) add(`items[${i}].target_repos must be an array of repo_id strings or null.`);
      if (target_repos && reposById) {
        for (const r of target_repos) if (!reposById.has(r)) add(`items[${i}].target_repos contains unknown repo_id '${r}'.`);
      }

      const acceptance_criteria = normalizeStringArray(s.acceptance_criteria);
      if (!acceptance_criteria) add(`items[${i}].acceptance_criteria must be an array of strings.`);
      if (acceptance_criteria && acceptance_criteria.length === 0) add(`items[${i}].acceptance_criteria must not be empty.`);

      const deps = isPlainObject(s.dependencies) ? s.dependencies : null;
      if (!deps) add(`items[${i}].dependencies must be an object.`);
      const must_run_after = deps ? normalizeStringArray(deps.must_run_after) : null;
      const can_run_in_parallel_with = deps ? normalizeStringArray(deps.can_run_in_parallel_with) : null;
      if (deps && must_run_after === null) add(`items[${i}].dependencies.must_run_after must be an array of strings.`);
      if (deps && can_run_in_parallel_with === null) add(`items[${i}].dependencies.can_run_in_parallel_with must be an array of strings.`);

      const ssot_refs = normalizeStringArray(s.ssot_refs);
      if (!ssot_refs) add(`items[${i}].ssot_refs must be an array of strings (use [] for none).`);

      const confidence = typeof s.confidence === "number" ? s.confidence : NaN;
      if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) add(`items[${i}].confidence must be a number between 0 and 1.`);

      if (seed_id && title && summary && rationale && Number.isInteger(phase) && phase >= 1 && priority && target_teams && acceptance_criteria && deps && ssot_refs && Number.isFinite(confidence)) {
        items.push({
          seed_id,
          title,
          summary,
          rationale,
          phase,
          priority,
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
      const ka = seedSortKey(a);
      const kb = seedSortKey(b);
      if (ka.phase !== kb.phase) return ka.phase - kb.phase;
      if (ka.prWeight !== kb.prWeight) return ka.prWeight - kb.prWeight;
      return String(ka.id).localeCompare(String(kb.id));
    };
    if (!isSortedBy(itemsRaw, cmp)) add("BACKLOG_SEEDS.json.items must be sorted by (phase asc, priority P0..P3, seed_id asc).");
  }

  if (errors.length) return { ok: false, errors, normalized: null };
  return { ok: true, errors: [], normalized: { version: 1, project_code, generated_at, items } };
}
