import { readTextIfExists, writeText } from "../../utils/fs.js";

function nowISO() {
  return new Date().toISOString();
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normalizeStringArray(v) {
  return Array.isArray(v) ? v.map((x) => String(x).trim()).filter(Boolean) : [];
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

export async function readMergedNotes(mergedPath, scope) {
  const existing = await readTextIfExists(mergedPath);
  if (!existing) {
    return {
      version: 1,
      scope,
      updated_at: nowISO(),
      sources: [],
      invariants: [],
      boundaries: [],
      constraints: [],
      risks: [],
      open_questions: [],
      decisions_needed: [],
    };
  }
  try {
    const parsed = JSON.parse(existing);
    if (!isPlainObject(parsed) || parsed.version !== 1) throw new Error("invalid");
    return parsed;
  } catch {
    return {
      version: 1,
      scope,
      updated_at: nowISO(),
      sources: [],
      invariants: [],
      boundaries: [],
      constraints: [],
      risks: [],
      open_questions: [],
      decisions_needed: [],
    };
  }
}

export async function mergeSessionNotesIntoMerged({
  mergedPath,
  scope,
  sessionFile,
  sessionNotesFile,
  sessionNotes,
}) {
  const merged = await readMergedNotes(mergedPath, scope);
  const notes = isPlainObject(sessionNotes) ? sessionNotes : {};

  merged.scope = scope;
  merged.updated_at = nowISO();

  const sources = Array.isArray(merged.sources) ? merged.sources.slice() : [];
  sources.push({
    recorded_at: nowISO(),
    session_file: String(sessionFile || "").trim() || null,
    session_notes_file: String(sessionNotesFile || "").trim() || null,
  });
  merged.sources = sources.filter((s) => isPlainObject(s));

  merged.invariants = dedupeByKey(
    [...(Array.isArray(merged.invariants) ? merged.invariants : []), ...(Array.isArray(notes.invariants) ? notes.invariants : [])].filter((x) => isPlainObject(x)),
    (x) => `${x.scope || ""}::${x.id || x.statement || ""}`,
  );
  merged.boundaries = dedupeByKey(
    [...(Array.isArray(merged.boundaries) ? merged.boundaries : []), ...(Array.isArray(notes.boundaries) ? notes.boundaries : [])].filter((x) => isPlainObject(x)),
    (x) => `${x.scope || ""}::${x.component || JSON.stringify(x)}`,
  );
  merged.constraints = dedupeByKey(
    [...(Array.isArray(merged.constraints) ? merged.constraints : []), ...(Array.isArray(notes.constraints) ? notes.constraints : [])].filter((x) => isPlainObject(x)),
    (x) => `${x.scope || ""}::${x.statement || JSON.stringify(x)}`,
  );
  merged.risks = dedupeByKey(
    [...(Array.isArray(merged.risks) ? merged.risks : []), ...(Array.isArray(notes.risks) ? notes.risks : [])].filter((x) => isPlainObject(x)),
    (x) => `${x.scope || ""}::${x.risk || JSON.stringify(x)}`,
  );
  merged.open_questions = dedupeByKey(
    [
      ...(Array.isArray(merged.open_questions) ? merged.open_questions : []).filter((x) => isPlainObject(x)),
      ...(Array.isArray(notes.open_questions) ? notes.open_questions : []).filter((x) => isPlainObject(x)),
    ],
    (x) => `${x.scope || ""}::${x.question || ""}`,
  );
  merged.decisions_needed = dedupeByKey(
    [...(Array.isArray(merged.decisions_needed) ? merged.decisions_needed : []), ...(Array.isArray(notes.decisions_needed) ? notes.decisions_needed : [])].filter((x) => isPlainObject(x)),
    (x) => `${x.scope || ""}::${x.question || JSON.stringify(x)}`,
  );

  await writeText(mergedPath, JSON.stringify(merged, null, 2) + "\n");
  return merged;
}

function renderList(items, renderItem) {
  const arr = Array.isArray(items) ? items : [];
  if (!arr.length) return ["- (none)"];
  const out = [];
  for (const it of arr) {
    const lines = renderItem(it);
    if (Array.isArray(lines) && lines.length) out.push(...lines);
  }
  return out.length ? out : ["- (none)"];
}

export function renderNotesMarkdown({ scope, merged }) {
  const inv = renderList(merged.invariants, (x) => {
    const id = String(x.id || "").trim() || null;
    const st = String(x.statement || "").trim();
    if (!st) return [];
    const rat = String(x.rationale || "").trim();
    return [`- ${id ? `**${id}**: ` : ""}${st}${rat ? ` — ${rat}` : ""}`];
  });

  const boundaries = renderList(merged.boundaries, (x) => {
    const comp = String(x.component || "").trim();
    if (!comp) return [];
    const owns = normalizeStringArray(x.owns);
    const notOwn = normalizeStringArray(x.does_not_own);
    const interfaces = normalizeStringArray(x.interfaces);
    const bits = [];
    bits.push(`- **${comp}**`);
    if (owns.length) bits.push(`  - owns: ${owns.join(", ")}`);
    if (notOwn.length) bits.push(`  - does_not_own: ${notOwn.join(", ")}`);
    if (interfaces.length) bits.push(`  - interfaces: ${interfaces.join(", ")}`);
    return bits;
  });

  const constraints = renderList(merged.constraints, (x) => {
    const st = String(x.statement || "").trim();
    if (!st) return [];
    const type = String(x.type || "").trim();
    const impact = String(x.impact || "").trim();
    return [`- ${type ? `**${type}**: ` : ""}${st}${impact ? ` — impact: ${impact}` : ""}`];
  });

  const risks = renderList(merged.risks, (x) => {
    const r = String(x.risk || "").trim();
    if (!r) return [];
    const sev = String(x.severity || "").trim() || "normal";
    const mit = String(x.mitigation || "").trim();
    return [`- **${sev}**: ${r}${mit ? ` — mitigation: ${mit}` : ""}`];
  });

  const openq = renderList(merged.open_questions, (x) => {
    if (!x || typeof x !== "object") return [];
    const s = String(x.question || "").trim();
    if (!s) return [];
    return [`- ${s}`];
  });

  const glossary = ["- (use ADRs or add glossary entries as needed)"];

  return {
    invariants_md: ["# Invariants", "", `Scope: ${scope}`, "", ...inv, ""].join("\n"),
    boundaries_md: ["# Boundaries", "", `Scope: ${scope}`, "", ...boundaries, ""].join("\n"),
    constraints_md: ["# Constraints", "", `Scope: ${scope}`, "", ...constraints, ""].join("\n"),
    risks_md: ["# Risks", "", `Scope: ${scope}`, "", ...risks, ""].join("\n"),
    open_questions_md: ["# Open Questions", "", `Scope: ${scope}`, "", ...openq, ""].join("\n"),
    glossary_md: ["# Glossary", "", `Scope: ${scope}`, "", ...glossary, ""].join("\n"),
  };
}

export function renderMergedNotesMd({ scope, merged }) {
  const lines = [];
  lines.push(`# Notes — ${scope}`);
  lines.push("");
  lines.push(`UpdatedAt: ${nowISO()}`);
  lines.push("");

  const section = (title, bodyLines) => {
    lines.push(`## ${title}`);
    lines.push("");
    if (!bodyLines.length) lines.push("- (none)");
    else lines.push(...bodyLines);
    lines.push("");
  };

  section(
    "Invariants",
    Array.isArray(merged?.invariants)
      ? merged.invariants
          .map((x) => (x && typeof x === "object" ? `- ${x.id ? `**${String(x.id).trim()}**: ` : ""}${String(x.statement || "").trim()}` : null))
          .filter((x) => x && x.trim())
      : [],
  );

  section(
    "Boundaries",
    Array.isArray(merged?.boundaries)
      ? merged.boundaries
          .map((x) => (x && typeof x === "object" && x.component ? `- **${String(x.component).trim()}**` : null))
          .filter((x) => x && x.trim())
      : [],
  );

  section(
    "Constraints",
    Array.isArray(merged?.constraints)
      ? merged.constraints
          .map((x) => (x && typeof x === "object" && x.statement ? `- ${String(x.type || "").trim() ? `**${String(x.type).trim()}**: ` : ""}${String(x.statement).trim()}` : null))
          .filter((x) => x && x.trim())
      : [],
  );

  section(
    "Risks",
    Array.isArray(merged?.risks)
      ? merged.risks
          .map((x) => (x && typeof x === "object" && x.risk ? `- **${String(x.severity || "normal").trim()}**: ${String(x.risk).trim()}` : null))
          .filter((x) => x && x.trim())
      : [],
  );

  section(
    "Open questions",
    Array.isArray(merged?.open_questions)
      ? merged.open_questions
          .map((x) => (x && typeof x === "object" && x.question ? `- ${String(x.question).trim()}` : null))
          .filter((x) => x && x.trim())
      : [],
  );

  section(
    "Decisions needed",
    Array.isArray(merged?.decisions_needed)
      ? merged.decisions_needed
          .map((x) => (x && typeof x === "object" && x.question ? `- ${String(x.question).trim()}` : null))
          .filter((x) => x && x.trim())
      : [],
  );

  return lines.join("\n");
}
