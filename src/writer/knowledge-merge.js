import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function stableSort(arr, keyFn) {
  return (Array.isArray(arr) ? arr : []).slice().sort((a, b) => String(keyFn(a)).localeCompare(String(keyFn(b))));
}

function dedupeByKey(items, keyFn) {
  const out = [];
  const seen = new Set();
  for (const it of Array.isArray(items) ? items : []) {
    const k = String(keyFn(it) || "").trim();
    if (!k) continue;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(it);
  }
  return out;
}

export async function loadMergedKnowledgeNotes({ knowledgeRepoPath, relPath }) {
  const repoAbs = resolve(String(knowledgeRepoPath || ""));
  const rel = String(relPath || "").trim();
  const abs = rel ? resolve(repoAbs, rel) : null;
  if (!abs || !existsSync(abs)) return { ok: false, missing: true, message: `Missing knowledge JSON: ${abs || "(no path)"}` };
  try {
    const text = await readFile(abs, "utf8");
    const parsed = JSON.parse(String(text || ""));
    if (!isPlainObject(parsed) || parsed.version !== 1) return { ok: false, message: `Invalid knowledge JSON: ${abs} (expected version=1).` };
    return { ok: true, path: abs, merged: parsed };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Failed to read knowledge JSON (${msg}).` };
  }
}

export function mergeMergedKnowledge({ base, add, updatedAtIso }) {
  const a = isPlainObject(base) ? base : {};
  const b = isPlainObject(add) ? add : {};
  const updated_at = typeof updatedAtIso === "string" && updatedAtIso.trim() ? updatedAtIso.trim() : new Date().toISOString();

  const out = {
    version: 1,
    scope: normStr(a.scope) || normStr(b.scope) || "all",
    updated_at,
    sources: dedupeByKey([...(Array.isArray(a.sources) ? a.sources : []), ...(Array.isArray(b.sources) ? b.sources : [])].filter(isPlainObject), (s) => s.session_file || JSON.stringify(s)),
    invariants: [],
    boundaries: [],
    constraints: [],
    risks: [],
    open_questions: [],
    decisions_needed: [],
  };

  out.invariants = dedupeByKey([...(Array.isArray(a.invariants) ? a.invariants : []), ...(Array.isArray(b.invariants) ? b.invariants : [])].filter(isPlainObject), (x) => `${x.scope || ""}::${x.id || x.statement || ""}`);
  out.boundaries = dedupeByKey([...(Array.isArray(a.boundaries) ? a.boundaries : []), ...(Array.isArray(b.boundaries) ? b.boundaries : [])].filter(isPlainObject), (x) => `${x.scope || ""}::${x.component || JSON.stringify(x)}`);
  out.constraints = dedupeByKey([...(Array.isArray(a.constraints) ? a.constraints : []), ...(Array.isArray(b.constraints) ? b.constraints : [])].filter(isPlainObject), (x) => `${x.scope || ""}::${x.type || ""}::${x.statement || JSON.stringify(x)}`);
  out.risks = dedupeByKey([...(Array.isArray(a.risks) ? a.risks : []), ...(Array.isArray(b.risks) ? b.risks : [])].filter(isPlainObject), (x) => `${x.scope || ""}::${x.severity || ""}::${x.risk || JSON.stringify(x)}`);
  out.open_questions = dedupeByKey([...(Array.isArray(a.open_questions) ? a.open_questions : []), ...(Array.isArray(b.open_questions) ? b.open_questions : [])].filter(isPlainObject), (x) => `${x.scope || ""}::${x.question || ""}`);
  out.decisions_needed = dedupeByKey([...(Array.isArray(a.decisions_needed) ? a.decisions_needed : []), ...(Array.isArray(b.decisions_needed) ? b.decisions_needed : [])].filter(isPlainObject), (x) => `${x.scope || ""}::${x.question || JSON.stringify(x)}`);

  return out;
}

function renderSection(title, lines) {
  const out = [];
  out.push(`## ${title}`);
  out.push("");
  if (!lines.length) out.push("- (none)");
  else out.push(...lines);
  out.push("");
  return out;
}

export function renderMergedNotesMarkdown({ projectKey, scope, merged, sources = [] }) {
  const out = [];
  out.push("# KNOWLEDGE");
  out.push("");
  out.push(`- project_key: ${projectKey}`);
  out.push(`- scope: ${scope}`);
  out.push(`- merged_scope: ${normStr(merged?.scope) || scope}`);
  out.push("");

  if (sources.length) {
    out.push("## Sources");
    out.push("");
    for (const s of sources) out.push(`- ${s}`);
    out.push("");
  }

  const invariants = stableSort(Array.isArray(merged?.invariants) ? merged.invariants.filter(isPlainObject) : [], (x) => x.id || x.statement || "");
  const boundaries = stableSort(Array.isArray(merged?.boundaries) ? merged.boundaries.filter(isPlainObject) : [], (x) => x.component || "");
  const constraints = stableSort(Array.isArray(merged?.constraints) ? merged.constraints.filter(isPlainObject) : [], (x) => `${x.type || ""}::${x.statement || ""}`);
  const risks = stableSort(Array.isArray(merged?.risks) ? merged.risks.filter(isPlainObject) : [], (x) => `${x.severity || ""}::${x.risk || ""}`);
  const openQuestions = stableSort(Array.isArray(merged?.open_questions) ? merged.open_questions.filter(isPlainObject) : [], (x) => x.question || "");
  const decisionsNeeded = stableSort(Array.isArray(merged?.decisions_needed) ? merged.decisions_needed.filter(isPlainObject) : [], (x) => x.question || "");

  out.push(
    ...renderSection(
      "Invariants",
      invariants
        .map((x) => {
          const id = normStr(x.id);
          const st = normStr(x.statement);
          const rat = normStr(x.rationale);
          if (!st) return null;
          return `- ${id ? `**${id}**: ` : ""}${st}${rat ? ` - ${rat}` : ""}`;
        })
        .filter(Boolean),
    ),
  );
  out.push(
    ...renderSection(
      "Boundaries",
      boundaries
        .map((x) => {
          const comp = normStr(x.component);
          if (!comp) return null;
          const owns = Array.isArray(x.owns) ? x.owns.map(normStr).filter(Boolean) : [];
          const notOwn = Array.isArray(x.does_not_own) ? x.does_not_own.map(normStr).filter(Boolean) : [];
          const ifaces = Array.isArray(x.interfaces) ? x.interfaces.map(normStr).filter(Boolean) : [];
          const lines = [];
          lines.push(`- **${comp}**`);
          if (owns.length) lines.push(`  - owns: ${owns.join(", ")}`);
          if (notOwn.length) lines.push(`  - does_not_own: ${notOwn.join(", ")}`);
          if (ifaces.length) lines.push(`  - interfaces: ${ifaces.join(", ")}`);
          return lines.join("\n");
        })
        .filter(Boolean),
    ),
  );
  out.push(
    ...renderSection(
      "Constraints",
      constraints
        .map((x) => {
          const st = normStr(x.statement);
          if (!st) return null;
          const type = normStr(x.type);
          const impact = normStr(x.impact);
          return `- ${type ? `**${type}**: ` : ""}${st}${impact ? ` - impact: ${impact}` : ""}`;
        })
        .filter(Boolean),
    ),
  );
  out.push(
    ...renderSection(
      "Risks",
      risks
        .map((x) => {
          const r = normStr(x.risk);
          if (!r) return null;
          const sev = normStr(x.severity) || "normal";
          const mit = normStr(x.mitigation);
          return `- **${sev}**: ${r}${mit ? ` - mitigation: ${mit}` : ""}`;
        })
        .filter(Boolean),
    ),
  );
  out.push(...renderSection("Open Questions", openQuestions.map((q) => `- ${normStr(q.question)}`).filter((x) => x.trim() !== "-")));
  out.push(
    ...renderSection(
      "Decisions Needed",
      decisionsNeeded
        .map((d) => {
          const q = normStr(d.question);
          if (!q) return null;
          const A = normStr(d.A);
          const B = normStr(d.B);
          const rec = d.recommended === "A" || d.recommended === "B" ? d.recommended : null;
          const bits = [];
          bits.push(`- Q: ${q}`);
          if (A) bits.push(`  - A: ${A}`);
          if (B) bits.push(`  - B: ${B}`);
          if (rec) bits.push(`  - Recommended: ${rec}`);
          return bits.join("\n");
        })
        .filter(Boolean),
    ),
  );

  return {
    markdown: out.join("\n"),
    open_questions: openQuestions.map((q) => normStr(q.question)).filter(Boolean),
    decisions_needed: decisionsNeeded.map((d) => normStr(d.question)).filter(Boolean),
  };
}

