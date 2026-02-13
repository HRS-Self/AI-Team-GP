import { existsSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { basename, join, resolve } from "node:path";
import readline from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";

import { loadProjectPaths } from "../../paths/project-paths.js";
import { validateScope } from "./knowledge-utils.js";
import {
  appendJsonlCapped,
  appendTextAtomic,
  assertKickoffLatestShape,
  kickoffSessionStem,
  readJsonAbs,
  stableKickoffQuestionId,
  writeTextAtomic,
} from "./kickoff-utils.js";

function nowISO() {
  return new Date().toISOString();
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normalizeStringArray(v) {
  if (v == null) return [];
  if (!Array.isArray(v)) throw new Error("Expected an array of strings.");
  const out = [];
  for (const x of v) {
    const s = normStr(x);
    if (!s) continue;
    if (!out.includes(s)) out.push(s);
  }
  return out;
}

function normalizeGlossary(v) {
  if (v == null) return [];
  if (!Array.isArray(v)) throw new Error("inputs.glossary must be an array.");
  const out = [];
  for (const x of v) {
    if (!isPlainObject(x)) throw new Error("inputs.glossary items must be objects.");
    const allowed = new Set(["term", "meaning"]);
    for (const k of Object.keys(x)) if (!allowed.has(k)) throw new Error(`inputs.glossary unknown field '${k}'.`);
    const term = normStr(x.term);
    const meaning = normStr(x.meaning);
    if (!term || !meaning) continue;
    const key = `${term}\n${meaning}`;
    if (out.some((g) => `${g.term}\n${g.meaning}` === key)) continue;
    out.push({ term, meaning });
  }
  out.sort((a, b) => a.term.localeCompare(b.term));
  return out;
}

function normalizeNfrs(v) {
  if (v == null) return [];
  if (!Array.isArray(v)) throw new Error("inputs.nfrs must be an array.");
  const out = [];
  for (const x of v) {
    if (!isPlainObject(x)) throw new Error("inputs.nfrs items must be objects.");
    const allowed = new Set(["name", "notes"]);
    for (const k of Object.keys(x)) if (!allowed.has(k)) throw new Error(`inputs.nfrs unknown field '${k}'.`);
    const name = normStr(x.name);
    const notes = normStr(x.notes);
    if (!name || !notes) continue;
    const key = `${name}\n${notes}`;
    if (out.some((n) => `${n.name}\n${n.notes}` === key)) continue;
    out.push({ name, notes });
  }
  out.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}

function normalizeMilestones(v) {
  if (v == null) return [];
  if (!Array.isArray(v)) throw new Error("inputs.milestones must be an array.");
  const out = [];
  for (const x of v) {
    if (!isPlainObject(x)) throw new Error("inputs.milestones items must be objects.");
    const allowed = new Set(["name", "target", "notes"]);
    for (const k of Object.keys(x)) if (!allowed.has(k)) throw new Error(`inputs.milestones unknown field '${k}'.`);
    const name = normStr(x.name);
    const target = normStr(x.target);
    const notes = normStr(x.notes);
    if (!name || !target) continue;
    if (!/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(target)) throw new Error(`inputs.milestones.target must be YYYY-MM-DD (got: ${target || "(empty)"}).`);
    const key = `${name}\n${target}\n${notes}`;
    if (out.some((m) => `${m.name}\n${m.target}\n${m.notes}` === key)) continue;
    out.push({ name, target, notes: notes || "" });
  }
  out.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}

function normalizeKickoffInputs(raw) {
  if (raw == null) {
    return {
      title: "",
      vision: "",
      problem_statement: "",
      stakeholders: [],
      in_scope: [],
      out_of_scope: [],
      constraints: [],
      assumptions: [],
      success_criteria: [],
      nfrs: [],
      milestones: [],
      glossary: [],
    };
  }

  if (!isPlainObject(raw)) throw new Error("Kickoff input must be a JSON object or a text file.");
  const allowed = new Set([
    "title",
    "vision",
    "problem_statement",
    "stakeholders",
    "in_scope",
    "out_of_scope",
    "constraints",
    "assumptions",
    "success_criteria",
    "nfrs",
    "milestones",
    "glossary",
  ]);
  for (const k of Object.keys(raw)) if (!allowed.has(k)) throw new Error(`inputs contains unknown field '${k}'.`);

  return {
    title: normStr(raw.title),
    vision: normStr(raw.vision),
    problem_statement: normStr(raw.problem_statement),
    stakeholders: normalizeStringArray(raw.stakeholders),
    in_scope: normalizeStringArray(raw.in_scope),
    out_of_scope: normalizeStringArray(raw.out_of_scope),
    constraints: normalizeStringArray(raw.constraints),
    assumptions: normalizeStringArray(raw.assumptions),
    success_criteria: normalizeStringArray(raw.success_criteria),
    nfrs: normalizeNfrs(raw.nfrs),
    milestones: normalizeMilestones(raw.milestones),
    glossary: normalizeGlossary(raw.glossary),
  };
}

const PHASES = [
  {
    phase: "vision",
    fields: [
      { key: "title", question: "What is the project title?", why: "We need a stable label for the effort and artifacts." },
      { key: "vision", question: "What is the vision (1–3 sentences)?", why: "Vision anchors all later scope and tradeoffs." },
      { key: "problem_statement", question: "What problem are we solving (pain + outcome)?", why: "Problem statement avoids feature-first wandering." },
      { key: "stakeholders", question: "Who are the stakeholders/users (comma-separated)?", why: "Stakeholders define priorities and success." },
    ],
    blocking: true,
  },
  {
    phase: "scope",
    fields: [
      { key: "in_scope", question: "What is in-scope (comma-separated items)?", why: "Defines what we will build/discover." },
      { key: "out_of_scope", question: "What is explicitly out-of-scope (comma-separated items)?", why: "Prevents over-discovery and scope creep." },
    ],
    blocking: true,
  },
  {
    phase: "constraints",
    fields: [
      { key: "constraints", question: "List constraints (tech/regulatory/time/budget/etc), comma-separated.", why: "Constraints gate solution space and risks." },
      { key: "assumptions", question: "List assumptions (comma-separated).", why: "Assumptions must be made explicit to avoid wrong conclusions." },
    ],
    blocking: true,
  },
  {
    phase: "glossary",
    fields: [{ key: "glossary", question: "Provide a domain glossary (term→meaning).", why: "Shared language prevents mismatch across repos/teams." }],
    blocking: false,
  },
  {
    phase: "success",
    fields: [{ key: "success_criteria", question: "List success criteria (comma-separated).", why: "Defines what ‘done’ means and avoids aimless work." }],
    blocking: true,
  },
  {
    phase: "nfr",
    fields: [{ key: "nfrs", question: "List NFRs (security, performance, reliability, etc.) with notes.", why: "NFRs affect architecture and acceptance criteria." }],
    blocking: false,
  },
  {
    phase: "milestone",
    fields: [{ key: "milestones", question: "List milestones with target dates.", why: "Milestones constrain planning and ordering." }],
    blocking: false,
  },
];

function isMissingField(inputs, key) {
  const v = inputs[key];
  if (typeof v === "string") return !normStr(v);
  if (Array.isArray(v)) return v.length === 0;
  return v == null;
}

function buildOpenQuestions({ scope, inputs, max = 25 }) {
  const out = [];
  for (const p of PHASES) {
    for (const f of p.fields) {
      if (!isMissingField(inputs, f.key)) continue;
      const question = f.question;
      const obj = {
        id: stableKickoffQuestionId({ scope, phase: p.phase, question }),
        phase: p.phase,
        question,
        why_needed: f.why,
        blocking: p.blocking,
      };
      out.push(obj);
      if (out.length >= max) break;
    }
    if (out.length >= max) break;
  }
  return out;
}

function evaluateSufficiency({ inputs, open_questions }) {
  const missingBlocking = (Array.isArray(open_questions) ? open_questions : []).filter((q) => q && q.blocking === true);
  if (missingBlocking.length > 0) {
    const phases = Array.from(new Set(missingBlocking.map((q) => String(q.phase)))).sort((a, b) => a.localeCompare(b));
    return { status: "insufficient", notes: `Missing required kickoff information in phase(s): ${phases.join(", ")}.` };
  }

  const missingNonBlocking = (Array.isArray(open_questions) ? open_questions : []).length;
  if (missingNonBlocking > 0) return { status: "partial", notes: "Core kickoff fields present; some non-blocking details are still missing." };
  return { status: "sufficient", notes: "Kickoff intent evidence is sufficient." };
}

function renderKickoffMd({ kickoff, mode, recordedAt, inputHint = null }) {
  const lines = [];
  lines.push(`# KICKOFF: ${kickoff.scope}`);
  lines.push("");
  lines.push(`created_at: ${kickoff.created_at}`);
  lines.push(`recorded_at: ${recordedAt}`);
  lines.push(`mode: ${mode}`);
  if (inputHint) lines.push(`input: ${inputHint}`);
  lines.push("");
  lines.push("## Inputs");
  lines.push("");
  const i = kickoff.inputs;
  lines.push(`- title: ${i.title || "-"}`);
  lines.push(`- vision: ${i.vision ? `${i.vision.slice(0, 200)}${i.vision.length > 200 ? "…" : ""}` : "-"}`);
  lines.push(`- problem_statement: ${i.problem_statement ? `${i.problem_statement.slice(0, 200)}${i.problem_statement.length > 200 ? "…" : ""}` : "-"}`);
  lines.push(`- stakeholders: ${(i.stakeholders || []).join(", ") || "-"}`);
  lines.push(`- in_scope: ${(i.in_scope || []).join(", ") || "-"}`);
  lines.push(`- out_of_scope: ${(i.out_of_scope || []).join(", ") || "-"}`);
  lines.push(`- constraints: ${(i.constraints || []).join(", ") || "-"}`);
  lines.push(`- assumptions: ${(i.assumptions || []).join(", ") || "-"}`);
  lines.push(`- success_criteria: ${(i.success_criteria || []).join(", ") || "-"}`);
  lines.push("");
  lines.push("## Open Questions");
  lines.push("");
  if (!kickoff.open_questions.length) lines.push("- (none)");
  for (const q of kickoff.open_questions) lines.push(`- [${q.phase}] ${q.question} (blocking=${q.blocking}) id=${q.id}`);
  lines.push("");
  lines.push("## Sufficiency");
  lines.push("");
  lines.push(`- status: ${kickoff.sufficiency.status}`);
  lines.push(`- notes: ${kickoff.sufficiency.notes}`);
  lines.push("");
  return lines.join("\n") + "\n";
}

function mergeInputs({ base, delta }) {
  const out = { ...base };
  for (const k of Object.keys(delta)) {
    const v = delta[k];
    if (typeof v === "string") {
      if (normStr(v)) out[k] = v;
      continue;
    }
    if (Array.isArray(v)) {
      if (v.length) out[k] = v;
      continue;
    }
    if (v && typeof v === "object") {
      if (Array.isArray(v) && v.length) out[k] = v;
      else if (Object.keys(v).length) out[k] = v;
    }
  }
  return out;
}

async function loadLatestOrNull(latestAbs) {
  if (!existsSync(latestAbs)) return null;
  const j = await readJsonAbs(latestAbs);
  return assertKickoffLatestShape(j);
}

async function parseInputFileOrText({ inputFileAbs, sessionText }) {
  if (inputFileAbs) {
    const t = await readFileOrThrow(inputFileAbs);
    const trimmed = String(t || "").trim();
    if (!trimmed) return { kind: "empty", raw: null };
    try {
      const obj = JSON.parse(trimmed);
      return { kind: "json", raw: obj };
    } catch {
      return { kind: "text", raw: trimmed };
    }
  }
  const text = normStr(sessionText);
  if (!text) return { kind: "empty", raw: null };
  try {
    const obj = JSON.parse(text);
    return { kind: "json", raw: obj };
  } catch {
    return { kind: "text", raw: text };
  }
}

async function readFileOrThrow(abs) {
  const { readFile } = await import("node:fs/promises");
  return readFile(resolve(abs), "utf8");
}

export async function runKnowledgeKickoff({
  projectRoot,
  scope = "system",
  start = false,
  cont = false,
  nonInteractive = false,
  inputFile = null,
  sessionText = null,
  maxQuestions = 12,
  kickoffDirName = "kickoff",
  dryRun = false,
} = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  const parsedScope = validateScope(scope);
  const scopeStr = parsedScope.scope;

  if ((start && cont) || (!start && !cont)) throw new Error("Must specify exactly one: --start or --continue.");

  const dirName = normStr(kickoffDirName) || "kickoff";
  if (!/^[A-Za-z0-9._-]+$/.test(dirName)) throw new Error("kickoffDirName must be a filesystem-safe token.");
  const kickoffDirAbs = join(paths.knowledge.sessionsAbs, dirName);
  const latestAbs = join(kickoffDirAbs, "LATEST.json");
  const historyAbs = join(kickoffDirAbs, "kickoff_history.jsonl");

  const inputFileAbs = inputFile ? resolve(String(inputFile)) : null;
  if (nonInteractive && !inputFileAbs && !normStr(sessionText)) throw new Error("Non-interactive kickoff requires --input-file or --session text.");

  const latest = (start || cont) && existsSync(latestAbs) ? await loadLatestOrNull(latestAbs) : null;
  if (cont && !latest) throw new Error(`Cannot --continue: sessions/${dirName}/LATEST.json is missing.`);
  const latestByScope = latest && latest.latest_by_scope && typeof latest.latest_by_scope === "object" ? latest.latest_by_scope : {};
  const latestEntry = cont ? latestByScope[scopeStr] : null;
  if (cont && !latestEntry) throw new Error(`Cannot --continue: no latest kickoff entry for scope ${scopeStr}. Use --start.`);

  const started = start ? kickoffSessionStem({ scope: scopeStr }) : null;
  const stem = started ? started.stem : null;
  const mdAbs = start ? join(kickoffDirAbs, `${stem}.md`) : join(kickoffDirAbs, String(latestEntry.latest_md));
  const jsonAbs = start ? join(kickoffDirAbs, `${stem}.json`) : join(kickoffDirAbs, String(latestEntry.latest_json));

  let prevKickoff = null;
  if (cont) prevKickoff = await readJsonAbs(jsonAbs);

  const parsedInput = nonInteractive ? await parseInputFileOrText({ inputFileAbs, sessionText }) : { kind: "empty", raw: null };

  let inputObj = null;
  if (parsedInput.kind === "json") {
    if (isPlainObject(parsedInput.raw) && isPlainObject(parsedInput.raw.inputs)) inputObj = parsedInput.raw.inputs;
    else if (isPlainObject(parsedInput.raw)) inputObj = parsedInput.raw;
    else throw new Error("Input JSON must be an object (or {inputs:{...}}).");
  }
  if (parsedInput.kind === "text") {
    inputObj = { problem_statement: String(parsedInput.raw || "") };
  }

  const baseInputs = cont && prevKickoff && isPlainObject(prevKickoff.inputs) ? normalizeKickoffInputs(prevKickoff.inputs) : normalizeKickoffInputs(null);
  const deltaInputs = normalizeKickoffInputs(inputObj);
  const mergedInputs = mergeInputs({ base: baseInputs, delta: deltaInputs });

  const questions = buildOpenQuestions({ scope: scopeStr, inputs: mergedInputs, max: 25 });

  let interactiveAnswers = [];
  if (!nonInteractive) {
    const rl = readline.createInterface({ input, output });
    try {
      const maxQ = Number.isFinite(maxQuestions) ? Math.max(0, Math.floor(maxQuestions)) : 12;
      interactiveAnswers = [];
      let asked = 0;
      for (const p of PHASES) {
        for (const f of p.fields) {
          if (asked >= maxQ) break;
          if (!isMissingField(mergedInputs, f.key)) continue;
          // eslint-disable-next-line no-await-in-loop
          const ans = await rl.question(`${f.question}\n> `);
          const a = normStr(ans);
          if (!a) continue;
          if (f.key === "stakeholders" || f.key === "in_scope" || f.key === "out_of_scope" || f.key === "constraints" || f.key === "assumptions" || f.key === "success_criteria") {
            mergedInputs[f.key] = a
              .split(",")
              .map((x) => normStr(x))
              .filter(Boolean);
          } else if (f.key === "glossary") {
            const m = a.match(/^([^:=]+)\\s*[:=]\\s*(.+)$/);
            if (m) mergedInputs.glossary = [{ term: normStr(m[1]), meaning: normStr(m[2]) }].filter((g) => g.term && g.meaning);
          } else if (f.key === "nfrs") {
            const m = a.match(/^([^:=]+)\\s*[:=]\\s*(.+)$/);
            if (m) mergedInputs.nfrs = [{ name: normStr(m[1]), notes: normStr(m[2]) }].filter((n) => n.name && n.notes);
          } else if (f.key === "milestones") {
            const parts = a.split(",").map((x) => normStr(x)).filter(Boolean);
            if (parts.length >= 2) {
              const name = parts[0];
              const target = parts[1];
              const notes = parts.slice(2).join(", ");
              if (/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(target)) mergedInputs.milestones = [{ name, target, notes }];
            }
          } else {
            mergedInputs[f.key] = a;
          }
          interactiveAnswers.push({ key: f.key, answer: a });
          asked += 1;
        }
        if (asked >= maxQ) break;
      }
    } finally {
      rl.close();
    }
  }

  const open_questions = buildOpenQuestions({ scope: scopeStr, inputs: mergedInputs, max: 25 });
  const sufficiency = evaluateSufficiency({ inputs: mergedInputs, open_questions });

  const recordedAt = nowISO();
  const createdAt = cont && prevKickoff && typeof prevKickoff.created_at === "string" && prevKickoff.created_at.trim() ? prevKickoff.created_at.trim() : recordedAt;

  const kickoff = {
    version: 1,
    created_at: createdAt,
    scope: scopeStr,
    inputs: mergedInputs,
    open_questions,
    sufficiency,
  };

  const latestSummary = {
    scope: scopeStr,
    created_at: kickoff.created_at,
    latest_md: basename(mdAbs),
    latest_json: basename(jsonAbs),
    sufficiency,
    open_questions_count: open_questions.length,
    blocking_questions_count: open_questions.filter((q) => q.blocking).length,
  };

  const latestObj = {
    version: 2,
    updated_at: recordedAt,
    latest_by_scope: {
      ...(latestByScope || {}),
      [scopeStr]: latestSummary,
    },
  };

  const mdChunk = renderKickoffMd({
    kickoff,
    mode: start ? "start" : "continue",
    recordedAt,
    inputHint:
      parsedInput.kind === "json"
        ? inputFileAbs
          ? `file:${inputFileAbs}`
          : "inline-json"
        : parsedInput.kind === "text"
          ? inputFileAbs
            ? `file:${inputFileAbs}`
            : "inline-text"
          : null,
  });

  const prevJsonText = cont && existsSync(jsonAbs) ? JSON.stringify(prevKickoff || {}, null, 2) + "\n" : null;
  const nextJsonText = JSON.stringify(kickoff, null, 2) + "\n";
  const changed = prevJsonText == null ? true : prevJsonText !== nextJsonText;

  if (!dryRun) {
    await mkdir(kickoffDirAbs, { recursive: true });
    if (start) {
      await writeTextAtomic(mdAbs, mdChunk);
    } else if (changed) {
      await appendTextAtomic(mdAbs, `\n---\n\n${mdChunk}`);
    }
    if (changed) await writeTextAtomic(jsonAbs, nextJsonText);
    if (changed) await writeTextAtomic(latestAbs, JSON.stringify(latestObj, null, 2) + "\n");
    if (changed) {
      await appendJsonlCapped(
        historyAbs,
        {
          version: 1,
          captured_at: recordedAt,
          scope: kickoff.scope,
          latest_json: `sessions/${dirName}/${basename(jsonAbs)}`,
          latest_md: `sessions/${dirName}/${basename(mdAbs)}`,
          sufficiency_status: sufficiency.status,
          open_questions_count: open_questions.length,
        },
        { maxLines: 500 },
      );
    }
  }

  const out = {
    ok: true,
    dry_run: dryRun,
    changed,
    scope: scopeStr,
    knowledge_root: paths.knowledge.rootAbs,
    kickoff_dir: kickoffDirAbs,
    latest_files: {
      latest: `sessions/${dirName}/LATEST.json`,
      md: `sessions/${dirName}/${basename(mdAbs)}`,
      json: `sessions/${dirName}/${basename(jsonAbs)}`,
      history: `sessions/${dirName}/kickoff_history.jsonl`,
    },
    sufficiency,
    open_questions_count: open_questions.length,
    next_action: open_questions.length
      ? { type: "kickoff", reason: "Answer open_questions and re-run --knowledge-kickoff --continue.", scope: scopeStr }
      : { type: "complete", reason: "Kickoff sufficient.", scope: scopeStr },
  };

  return out;
}
