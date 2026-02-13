import { computeStableGapId } from "../lane_a/knowledge/gap-id.js";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function normLower(x) {
  return normStr(x).toLowerCase();
}

function normUpper(x) {
  return normStr(x).toUpperCase();
}

export { computeStableGapId };

export function validateKnowledgeGap(gap) {
  const errors = [];
  const add = (m) => errors.push(String(m));
  if (!isPlainObject(gap)) return { ok: false, errors: ["gap must be an object"], normalized: null };

  const scope = normStr(gap.scope);
  if (!scope) add("scope is required");
  const category = normLower(gap.category);
  const allowedCategory = new Set([
    "feature_missing",
    "integration_missing",
    "contract_mismatch",
    "behavior_mismatch",
    "nfr_gap",
    "security_gap",
  ]);
  if (!allowedCategory.has(category)) add("category is invalid");

  const severity = normLower(gap.severity);
  const risk = normLower(gap.risk);
  const allowedLevel = new Set(["high", "medium", "low"]);
  if (!allowedLevel.has(severity)) add("severity is invalid");
  if (!allowedLevel.has(risk)) add("risk is invalid");

  const summary = normStr(gap.summary);
  const expected = normStr(gap.expected);
  const observed = normStr(gap.observed);
  if (!summary) add("summary is required");
  if (!expected) add("expected is required");
  if (!observed) add("observed is required");

  const evidenceIn = Array.isArray(gap.evidence) ? gap.evidence : [];
  const evidence = [];
  for (const e of evidenceIn) {
    if (!isPlainObject(e)) continue;
    const type = normLower(e.type);
    if (!type) continue;
    if (type === "file") {
      const path = normStr(e.path);
      if (!path) continue;
      if (path.startsWith("/") || path.includes("..") || path.includes("\\")) continue;
      evidence.push({ type: "file", path, hint: normStr(e.hint) || null });
      continue;
    }
    if (type === "grep") {
      const pattern = normStr(e.pattern);
      const hits = Number.isFinite(Number(e.hits)) ? Number(e.hits) : null;
      if (!pattern) continue;
      evidence.push({ type: "grep", pattern, hits: Number.isFinite(hits) ? hits : null });
      continue;
    }
    if (type === "endpoint") {
      const method = normUpper(e.method) || "GET";
      const path = normStr(e.path);
      if (!path) continue;
      evidence.push({ type: "endpoint", method, path });
      continue;
    }
  }
  if (!evidence.length) add("evidence is required");

  const suggested = isPlainObject(gap.suggested_intake) ? gap.suggested_intake : null;
  const suggested_repo_id = suggested ? normStr(suggested.repo_id) : "";
  const suggested_title = suggested ? normStr(suggested.title) : "";
  const suggested_body = suggested ? normStr(suggested.body) : "";
  const suggested_labels = suggested && Array.isArray(suggested.labels) ? suggested.labels.map(normStr).filter(Boolean) : [];
  const labelsSet = new Set(suggested_labels.map((x) => x.toLowerCase()));

  if (!suggested) add("suggested_intake is required");
  if (suggested && !suggested_repo_id) add("suggested_intake.repo_id is required");
  if (suggested && !suggested_title) add("suggested_intake.title is required");
  if (suggested && !suggested_body) add("suggested_intake.body is required");
  if (suggested && (!Array.isArray(suggested.labels) || !suggested_labels.length)) add("suggested_intake.labels is required");
  if (suggested && !labelsSet.has("gap")) add("suggested_intake.labels must include 'gap'");
  if (suggested && !labelsSet.has("ai")) add("suggested_intake.labels must include 'ai'");

  const gap_id = computeStableGapId({ ...gap, scope, category, summary, expected, observed, evidence });

  if (errors.length) return { ok: false, errors, normalized: null };
  return {
    ok: true,
    errors: [],
    normalized: {
      gap_id,
      scope,
      category,
      severity,
      risk,
      summary,
      expected,
      observed,
      evidence,
      suggested_intake: {
        repo_id: suggested_repo_id,
        title: suggested_title,
        body: suggested_body,
        labels: suggested_labels.slice().sort((a, b) => a.localeCompare(b)),
      },
    },
  };
}

export function validateKnowledgeGapsFile(json) {
  const errors = [];
  const add = (m) => errors.push(String(m));
  if (!isPlainObject(json)) return { ok: false, errors: ["gaps JSON must be an object"], normalized: null };
  const version = Number.isFinite(Number(json.version)) ? Number(json.version) : null;
  if (version !== 1) add("version must be 1");
  const scope = normStr(json.scope);
  if (!scope) add("scope is required");
  const captured_at = normStr(json.captured_at);
  if (!captured_at) add("captured_at is required");
  const extractor_version = normStr(json.extractor_version);
  if (!extractor_version) add("extractor_version is required");
  const gapsIn = Array.isArray(json.gaps) ? json.gaps : null;
  if (!gapsIn) add("gaps must be an array");

  const gaps = [];
  if (Array.isArray(gapsIn)) {
    for (const g of gapsIn) {
      const v = validateKnowledgeGap(g);
      if (!v.ok) {
        for (const e of v.errors) add(`gap: ${e}`);
        continue;
      }
      gaps.push(v.normalized);
    }
  }

  if (errors.length) return { ok: false, errors, normalized: null };
  const deduped = [];
  const seen = new Set();
  for (const g of gaps.sort((a, b) => a.gap_id.localeCompare(b.gap_id))) {
    if (seen.has(g.gap_id)) continue;
    seen.add(g.gap_id);
    deduped.push(g);
  }
  return {
    ok: true,
    errors: [],
    normalized: {
      version: 1,
      scope,
      captured_at,
      extractor_version,
      gaps: deduped,
    },
  };
}
