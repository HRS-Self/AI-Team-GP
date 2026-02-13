import { createHash } from "node:crypto";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
}

function isStringArray(x) {
  return Array.isArray(x) && x.every((v) => typeof v === "string");
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function normalizeBranchName(x) {
  if (x === null) return null;
  if (!isNonEmptyString(x)) return null;
  const s = x.trim();
  // Keep as-is (branch names are case sensitive in git), but disallow obvious filesystem paths.
  if (s.startsWith("/")) return null;
  if (s.includes("..")) return null;
  return s;
}

function normalizeStringArrayOrEmpty(x) {
  if (!Array.isArray(x)) return [];
  return x.map((v) => String(v)).map((v) => v.trim()).filter(Boolean);
}

function normalizeAcceptanceCriteria(arr) {
  const out = normalizeStringArrayOrEmpty(arr);
  return out.length ? out : [];
}

function stableTaskId({ source_intake_id, title, description, index }) {
  return sha256Hex([String(source_intake_id || ""), String(index || 0), String(title || ""), String(description || "")].join("\n")).slice(0, 16);
}

function remapTaskDependencyIds({ tasks, rawDepIds }) {
  const deps = normalizeStringArrayOrEmpty(rawDepIds);
  if (!deps.length) return [];

  const byIndex = new Map();
  const byTitle = new Map();
  const byRawId = new Map();

  tasks.forEach((t, idx) => {
    byIndex.set(String(idx + 1), t.task_id);
    byIndex.set(`T${idx + 1}`, t.task_id);
    byTitle.set(String(t.title || "").trim().toLowerCase(), t.task_id);
    byRawId.set(String(t._raw_task_id || "").trim(), t.task_id);
  });

  const out = [];
  for (const d of deps) {
    const k = String(d).trim();
    const low = k.toLowerCase();
    const mapped = byRawId.get(k) || byIndex.get(k) || byIndex.get(low.toUpperCase()) || byTitle.get(low) || null;
    if (mapped && !out.includes(mapped)) out.push(mapped);
  }
  return out;
}

export function validateTriageOutput(raw, { sourceIntakeId = null, triageId = null, createdAt = null } = {}) {
  const errors = [];
  const add = (msg) => errors.push(msg);

  if (!isPlainObject(raw)) return { ok: false, errors: ["Triage output must be a JSON object."], normalized: null };

  const version = raw.version === 1 ? 1 : null;
  if (version !== 1) add("version must be 1.");

  const source_intake_id = sourceIntakeId || (isNonEmptyString(raw.source_intake_id) ? raw.source_intake_id.trim() : null);
  if (!isNonEmptyString(source_intake_id)) add("source_intake_id must be a non-empty string.");

  const triage_id = triageId || (isNonEmptyString(raw.triage_id) ? raw.triage_id.trim() : null);
  if (!isNonEmptyString(triage_id)) add("triage_id must be a non-empty string.");

  const createdAtValue = createdAt || (isNonEmptyString(raw.createdAt) ? raw.createdAt.trim() : null);
  if (!isNonEmptyString(createdAtValue)) add("createdAt must be a non-empty string.");

  const confidence_overall = typeof raw.confidence_overall === "number" ? raw.confidence_overall : null;
  if (!(typeof confidence_overall === "number" && confidence_overall >= 0 && confidence_overall <= 1)) add("confidence_overall must be a number in [0,1].");

  const questions_for_human = normalizeStringArrayOrEmpty(raw.questions_for_human);

  // tasks
  const rawTasks = Array.isArray(raw.tasks) ? raw.tasks : null;
  if (!Array.isArray(rawTasks) || rawTasks.length === 0) add("tasks must be a non-empty array.");

  const tasks = [];
  if (Array.isArray(rawTasks)) {
    rawTasks.forEach((t, i) => {
      if (!isPlainObject(t)) {
        add(`tasks[${i}] must be an object.`);
        return;
      }

      const title = isNonEmptyString(t.title) ? t.title.trim() : null;
      const description = isNonEmptyString(t.description) ? t.description.trim() : null;
      if (!title) add(`tasks[${i}].title must be a non-empty string.`);
      if (!description) add(`tasks[${i}].description must be a non-empty string.`);

      const suggested_repo_ids = normalizeStringArrayOrEmpty(t.suggested_repo_ids);
      const suggested_team_ids = normalizeStringArrayOrEmpty(t.suggested_team_ids);

      const target_branch = normalizeBranchName(t.target_branch ?? null);

      const acceptance_criteria = normalizeAcceptanceCriteria(t.acceptance_criteria);
      if (!acceptance_criteria.length) add(`tasks[${i}].acceptance_criteria must be a non-empty string array.`);

      const risk_level = isNonEmptyString(t.risk_level) ? t.risk_level.trim().toLowerCase() : null;
      if (!(risk_level === "low" || risk_level === "normal" || risk_level === "high")) add(`tasks[${i}].risk_level must be low|normal|high.`);

      const depsObj = isPlainObject(t.dependencies) ? t.dependencies : null;
      if (!depsObj) add(`tasks[${i}].dependencies must be an object.`);
      const depends_on_workIds = depsObj ? normalizeStringArrayOrEmpty(depsObj.depends_on_workIds) : [];
      const depends_on_task_ids = depsObj ? normalizeStringArrayOrEmpty(depsObj.depends_on_task_ids) : [];

      const rawTaskId = isNonEmptyString(t.task_id) ? t.task_id.trim() : null;
      const computedTaskId = stableTaskId({ source_intake_id, title, description, index: i + 1 });
      // Always compute deterministically; LLM-provided IDs are not reliably stable.
      const task_id = computedTaskId;

      tasks.push({
        task_id,
        title: title || "(missing)",
        description: description || "(missing)",
        suggested_repo_ids,
        suggested_team_ids,
        target_branch,
        acceptance_criteria,
        risk_level: risk_level || "normal",
        dependencies: {
          depends_on_workIds,
          // remapped after all tasks collected
          depends_on_task_ids,
        },
        _raw_task_id: rawTaskId,
      });
    });
  }

  // Remap depends_on_task_ids deterministically.
  for (const t of tasks) {
    t.dependencies.depends_on_task_ids = remapTaskDependencyIds({ tasks, rawDepIds: t.dependencies.depends_on_task_ids });
    delete t._raw_task_id;
  }

  // dedupe
  const dedupe = isPlainObject(raw.dedupe) ? raw.dedupe : null;
  if (!dedupe) add("dedupe must be an object.");
  const possible_duplicates = Array.isArray(dedupe?.possible_duplicates) ? dedupe.possible_duplicates : [];
  const normalizedDuplicates = [];
  for (const [idx, d] of possible_duplicates.entries()) {
    if (!isPlainObject(d)) {
      add(`dedupe.possible_duplicates[${idx}] must be an object.`);
      continue;
    }
    const workId = isNonEmptyString(d.workId) ? d.workId.trim() : null;
    const reason = isNonEmptyString(d.reason) ? d.reason.trim() : null;
    const conf = typeof d.confidence === "number" ? d.confidence : null;
    if (!workId) add(`dedupe.possible_duplicates[${idx}].workId must be a non-empty string.`);
    if (!reason) add(`dedupe.possible_duplicates[${idx}].reason must be a non-empty string.`);
    if (!(typeof conf === "number" && conf >= 0 && conf <= 1)) add(`dedupe.possible_duplicates[${idx}].confidence must be a number in [0,1].`);
    if (workId && reason && typeof conf === "number") normalizedDuplicates.push({ workId, reason, confidence: conf });
  }

  const normalized = {
    version: 1,
    source_intake_id,
    triage_id,
    createdAt: createdAtValue,
    tasks: tasks.map((t) => ({
      task_id: t.task_id,
      title: t.title,
      description: t.description,
      suggested_repo_ids: t.suggested_repo_ids,
      suggested_team_ids: t.suggested_team_ids,
      target_branch: t.target_branch,
      acceptance_criteria: t.acceptance_criteria,
      risk_level: t.risk_level,
      dependencies: {
        depends_on_workIds: t.dependencies.depends_on_workIds,
        depends_on_task_ids: t.dependencies.depends_on_task_ids,
      },
    })),
    dedupe: { possible_duplicates: normalizedDuplicates },
    confidence_overall,
    questions_for_human,
  };

  // Validate normalized is still complete.
  if (!normalized.tasks.length) add("tasks must be a non-empty array.");
  for (const [i, t] of normalized.tasks.entries()) {
    if (!isNonEmptyString(t.task_id)) add(`tasks[${i}].task_id must be a non-empty string.`);
    if (!isNonEmptyString(t.title)) add(`tasks[${i}].title must be a non-empty string.`);
    if (!isNonEmptyString(t.description)) add(`tasks[${i}].description must be a non-empty string.`);
    if (!isStringArray(t.acceptance_criteria) || !t.acceptance_criteria.length) add(`tasks[${i}].acceptance_criteria must be a non-empty string array.`);
    if (!(t.risk_level === "low" || t.risk_level === "normal" || t.risk_level === "high")) add(`tasks[${i}].risk_level must be low|normal|high.`);
    if (!isPlainObject(t.dependencies)) add(`tasks[${i}].dependencies must be an object.`);
    if (!isStringArray(t.dependencies.depends_on_workIds)) add(`tasks[${i}].dependencies.depends_on_workIds must be string[].`);
    if (!isStringArray(t.dependencies.depends_on_task_ids)) add(`tasks[${i}].dependencies.depends_on_task_ids must be string[].`);
  }

  return { ok: errors.length === 0, errors, normalized };
}
