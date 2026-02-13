import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { createLlmClient } from "../../llm/client.js";
import { normalizeLlmContentToText } from "../../llm/content.js";
import { sha256Hex } from "./knowledge-utils.js";

function nowISO() {
  return new Date().toISOString();
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normalizeStringArray(v) {
  return Array.isArray(v) ? v.map((x) => String(x).trim()).filter(Boolean) : [];
}

const SDLC_STAGES = ["VISION", "REQUIREMENTS", "DOMAIN_DATA", "API", "INFRA", "OPS"];

function stageRank(stage) {
  const idx = SDLC_STAGES.indexOf(String(stage || "").trim());
  return idx < 0 ? null : idx;
}

export function computeAllowedSdlcStageFromContext(sdlcContext) {
  if (!isPlainObject(sdlcContext)) return null;
  const explicit = typeof sdlcContext.allowed_stage === "string" ? sdlcContext.allowed_stage.trim() : "";
  if (explicit && SDLC_STAGES.includes(explicit)) return explicit;

  const completion = isPlainObject(sdlcContext.completion) ? sdlcContext.completion : null;
  if (!completion) return null;
  const vision = completion.vision === true;
  const requirements = completion.requirements === true;
  const domain = completion.domain_data === true;
  const api = completion.api === true;
  const infra = completion.infra === true;
  const ops = completion.ops === true;

  if (!vision) return "VISION";
  if (!requirements) return "REQUIREMENTS";
  if (!domain) return "DOMAIN_DATA";
  if (!api) return "API";
  if (!infra) return "INFRA";
  if (!ops) return "OPS";
  return "OPS";
}

function validateEvidenceMissingStrings(arr) {
  const items = Array.isArray(arr) ? arr : [];
  const out = [];
  for (const x of items) {
    const s = typeof x === "string" ? x.trim() : "";
    if (!s) continue;
    if (s.length < 12) throw new Error("evidence_missing entries must be descriptive (min length 12 chars).");
    // Reject opaque hashes/IDs unless contextualized (file:/path:/endpoint:).
    if (/^[a-f0-9]{16,}$/i.test(s) && !/(file:|path:|endpoint:)/i.test(s)) {
      throw new Error("evidence_missing entries must not be opaque IDs/hashes without context (include file:/path:/endpoint:).");
    }
    out.push(s);
  }
  return Array.from(new Set(out)).sort((a, b) => a.localeCompare(b));
}

export function validateKnowledgeInterviewerOutput(raw, { sdlcContext = null } = {}) {
  const errors = [];
  const add = (m) => errors.push(String(m));

  if (!isPlainObject(raw)) return { ok: false, errors: ["LLM output must be a JSON object."], normalized: null };

  const allowedTop = new Set(["scope", "stage", "questions", "known_facts", "assumptions", "unknowns"]);
  for (const k of Object.keys(raw)) {
    if (!allowedTop.has(k)) add(`unknown top-level key '${k}'`);
  }

  const scope = typeof raw.scope === "string" ? raw.scope.trim() : "";
  if (!scope) add("scope must be a non-empty string (expected: system or repo:<repo_id>).");

  const stage = typeof raw.stage === "string" ? raw.stage.trim() : "";
  if (!SDLC_STAGES.includes(stage)) add(`stage must be one of: ${SDLC_STAGES.join(", ")}`);

  const questionsRaw = Array.isArray(raw.questions) ? raw.questions : null;
  if (!questionsRaw) add("questions must be an array.");

  const questions = [];
  if (Array.isArray(questionsRaw)) {
    for (const q of questionsRaw) {
      if (!isPlainObject(q)) continue;
      const qAllowed = new Set(["id", "text", "why_now", "blocks", "evidence_refs", "evidence_missing"]);
      for (const k of Object.keys(q)) if (!qAllowed.has(k)) add(`questions[] unknown key '${k}'`);

      const id = typeof q.id === "string" ? q.id.trim() : "";
      const text = typeof q.text === "string" ? q.text.trim() : "";
      const why_now = typeof q.why_now === "string" ? q.why_now.trim() : "";
      const blocks = Array.isArray(q.blocks) ? q.blocks.map((b) => String(b).trim()).filter(Boolean) : [];
      const evidence_refs = Array.isArray(q.evidence_refs) ? q.evidence_refs.map((e) => String(e).trim()).filter(Boolean) : [];
      let evidence_missing = [];
      try {
        evidence_missing = validateEvidenceMissingStrings(q.evidence_missing);
      } catch (err) {
        add(`questions[].evidence_missing invalid: ${err instanceof Error ? err.message : String(err)}`);
      }

      if (!id) add("questions[].id must be a non-empty string.");
      if (!text) add("questions[].text must be a non-empty string.");
      if (!why_now) add("questions[].why_now must be a non-empty string.");
      if (!Array.isArray(q.blocks)) add("questions[].blocks must be an array.");
      if (!Array.isArray(q.evidence_refs)) add("questions[].evidence_refs must be an array.");
      if (!Array.isArray(q.evidence_missing)) add("questions[].evidence_missing must be an array (use [] if none).");

      questions.push({ id, text, why_now, blocks, evidence_refs, evidence_missing });
    }
  }

  const known_facts = normalizeStringArray(raw.known_facts);
  const assumptions = normalizeStringArray(raw.assumptions);
  const unknowns = normalizeStringArray(raw.unknowns);

  const allowedStage = computeAllowedSdlcStageFromContext(sdlcContext);
  if (allowedStage && stage) {
    if (stage !== allowedStage) add(`stage violates SDLC ladder: allowed_stage=${allowedStage} got=${stage}`);
    const gotRank = stageRank(stage);
    const allowRank = stageRank(allowedStage);
    if (gotRank == null || allowRank == null) add("invalid stage ordering.");
  }

  const normalized = errors.length
    ? null
    : {
        scope,
        stage,
        questions,
        known_facts,
        assumptions,
        unknowns,
      };

  return { ok: errors.length === 0, errors, normalized };
}

function normalizeScopedStringItems(v, { defaultScope, fieldName }) {
  const out = [];
  for (const it of Array.isArray(v) ? v : []) {
    if (typeof it === "string") {
      const q = it.trim();
      if (!q) continue;
      out.push({ scope: defaultScope, [fieldName]: q });
      continue;
    }
    if (!isPlainObject(it)) continue;
    const scope = typeof it.scope === "string" && it.scope.trim() ? it.scope.trim() : defaultScope;
    const val = typeof it[fieldName] === "string" ? it[fieldName].trim() : "";
    if (!val) continue;
    out.push({ scope, [fieldName]: val });
  }
  return out;
}

function attachScopeToNoteObjects(items, defaultScope) {
  const out = [];
  for (const it of Array.isArray(items) ? items : []) {
    if (!isPlainObject(it)) continue;
    const scope = typeof it.scope === "string" && it.scope.trim() ? it.scope.trim() : defaultScope;
    out.push({ ...it, scope });
  }
  return out;
}

function classifyLlmError(err) {
  const msg = err instanceof Error ? err.message : String(err);
  const lower = msg.toLowerCase();
  if (lower.includes("timed out")) return { error_type: "timeout", retryable: true, message: msg };
  if (lower.includes("missing openai_api_key") || lower.includes("invalid api key") || lower.includes("unauthorized") || lower.includes("401")) {
    return { error_type: "auth", retryable: false, message: msg };
  }
  if (lower.includes("rate limit") || lower.includes("429")) return { error_type: "rate_limit", retryable: true, message: msg };
  return { error_type: "unknown", retryable: true, message: msg };
}

function loadSystemPrompt() {
  const p = resolve("src/llm/prompts/knowledge-interviewer.system.txt");
  return readFileSync(p, "utf8");
}

function validateSessionOutput(raw, { sdlcContext = null } = {}) {
  const v = validateKnowledgeInterviewerOutput(raw, { sdlcContext });
  if (!v.ok) return { ok: false, errors: v.errors, normalized: null };

  const scope = v.normalized.scope;
  const stage = v.normalized.stage;

  const questions = v.normalized.questions.map((q) => ({
    question: q.text,
    answer: null,
  }));

  const findings_summary = {
    known: v.normalized.known_facts,
    uncertain: v.normalized.assumptions,
    decisions_needed: [],
  };

  const mkId = (prefix, text) => `${prefix}-${sha256Hex(`${scope}\n${prefix}\n${String(text || "")}`).slice(0, 12)}`;

  const invariants = v.normalized.known_facts.map((text) => ({
    scope,
    id: mkId("FACT", text),
    statement: text,
    rationale: "evidence-backed (interview context)",
  }));
  const constraints = v.normalized.assumptions.map((text) => ({
    scope,
    type: "assumption",
    statement: text,
    impact: "",
  }));

  const open_questions = []
    .concat(v.normalized.unknowns.map((u) => ({ scope, question: u })))
    .concat(v.normalized.questions.map((q) => ({ scope, question: q.text })));

  const out = {
    scope,
    stage,
    questions,
    findings_summary,
    session_notes: {
      invariants,
      boundaries: [],
      constraints,
      risks: [],
      open_questions,
      decisions_needed: [],
    },
    backlog_seeds: null,
    gaps: null,
  };

  return { ok: true, errors: [], normalized: out };
}

export function renderTranscriptMd({ scope, sessionId, charterTitle, questions, userSessionText }) {
  const lines = [];
  lines.push(`# Architect Interview — ${scope}`);
  lines.push("");
  lines.push(`Session: ${sessionId}`);
  if (charterTitle) lines.push(`Charter: ${charterTitle}`);
  lines.push(`RecordedAt: ${nowISO()}`);
  lines.push("");
  lines.push("## Q/A");
  lines.push("");
  if (!questions.length) {
    lines.push("- (no questions)");
  } else {
    let i = 1;
    for (const qa of questions) {
      lines.push(`### Q${i}`);
      lines.push("");
      lines.push(qa.question);
      lines.push("");
      lines.push("**Answer**");
      lines.push("");
      lines.push(qa.answer ? qa.answer : "(pending)");
      lines.push("");
      i += 1;
    }
  }
  lines.push("");
  lines.push("## Raw session input");
  lines.push("");
  lines.push("```");
  lines.push(String(userSessionText || "").trimEnd());
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}

export async function runArchitectInterview({
  scope,
  charterMd,
  mergedNotesJson,
  previousQuestions = [],
  userSessionText,
  maxQuestions = 12,
  llmConfig = null,
  timeoutMs = 60_000,
  systemPrompt = null,
  sdlcContext = null,
}) {
  // Test-only deterministic stub (no network / no OpenAI dependency).
  // Used by `npm run verify:knowledge` to validate pathing + git commits.
  if (String(process.env.KNOWLEDGE_TEST_STUB || "").trim() === "1" || String(process.env.KNOWLEDGE_TEST_STUB || "").trim().toLowerCase() === "true") {
    const stableId = `S-${sha256Hex(`${scope}\n${userSessionText || ""}`).slice(0, 12)}`;
    return {
      ok: true,
      model: "stub",
      sessionId: stableId,
      scope,
      questions: [
        { question: "List the most important invariants and constraints.", answer: String(userSessionText || "").trim() || "(none)" },
      ],
      session_notes: {
        invariants: [{ scope, id: "INV-001", statement: "Knowledge is stored in the knowledge repo, not runtime.", rationale: "Runtime folder is ephemeral." }],
        boundaries: [],
        constraints: [{ scope, type: "operational", statement: "No repo code edits from interviewer.", impact: "Interview must stay read-only for SDLC repos." }],
        risks: [{ scope, risk: "Knowledge repo not configured", severity: "high", mitigation: "Run --initial-project and initialize/set origin." }],
        open_questions: [{ scope, question: "What is the canonical scope for this interview (system or a specific repo)?" }],
        decisions_needed: [{ scope, question: "Should the knowledge repo auto-push on every run?", A: "Yes (default)", B: "No (manual)", recommended: "A" }],
      },
      backlog_seeds: {
        items: [
          {
            seed_id: "SEED-001",
            title: "Establish baseline SSOT structure",
            summary: "Create and validate initial SSOT snapshot, views, and section skeletons.",
            rationale: "Delivery must be deterministic and pinned to SSOT sections before any code changes.",
            phase: 1,
            priority: "P0",
            target_teams: ["Tooling"],
            target_repos: null,
            acceptance_criteria: ["PROJECT_SNAPSHOT.json exists and validates", "global view exists", "sections exist and match sha256 in snapshot"],
            dependencies: { must_run_after: [], can_run_in_parallel_with: [] },
            ssot_refs: [],
            confidence: 0.5,
          },
        ],
      },
      gaps: {
        baseline: "test baseline",
        items: [
          {
            gap_id: "GAP-001",
            title: "Missing CI-only validation contract",
            summary: "Current pipeline may depend on local checks; CI must be the only truth post-PR.",
            observed_evidence: ["verify stub evidence"],
            impact: "high",
            risk_level: "high",
            recommended_action: "Ensure delivery lane treats CI as the only success signal.",
            target_teams: ["Tooling"],
            target_repos: null,
            acceptance_criteria: ["No local lint/build/test is used as success signal", "CI status is recorded deterministically"],
            dependencies: { must_run_after: [], can_run_in_parallel_with: [] },
            ssot_refs: [],
            confidence: 0.5,
          },
        ],
      },
    };
  }

  const sys = typeof systemPrompt === "string" && systemPrompt.trim().length ? systemPrompt : loadSystemPrompt();
  const provider = llmConfig && typeof llmConfig.provider === "string" ? llmConfig.provider : null;
  const modelIn = llmConfig && typeof llmConfig.model === "string" ? llmConfig.model : null;
  const { ok, llm, model, message } = createLlmClient({ ...(llmConfig && typeof llmConfig === "object" ? llmConfig : {}), provider, model: modelIn, timeoutMs });
  if (!ok) return { ok: false, error: { error_type: "auth", retryable: false, message }, model: modelIn || null };

  const payload = {
    scope,
    max_questions: maxQuestions,
    charter_md: String(charterMd || ""),
    merged_notes_json: mergedNotesJson || null,
    previous_questions: previousQuestions,
    user_session_text: String(userSessionText || ""),
    sdlc_context: sdlcContext || null,
  };

  const messages = [
    { role: "system", content: sys },
    {
      role: "user",
      content: JSON.stringify(payload, null, 2),
    },
  ];

  try {
    const res = await llm.invoke(messages);
    const norm = normalizeLlmContentToText(res?.content);
    const text = norm.text;
    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch {
      return {
        ok: false,
        error: { error_type: "malformed", retryable: false, message: "LLM output was not valid JSON." },
        model,
        raw: text.slice(0, 2000),
      };
    }

    const v = validateSessionOutput(parsed, { sdlcContext });
    if (!v.ok) {
      return {
        ok: false,
        error: { error_type: "malformed", retryable: false, message: `LLM output failed validation: ${v.errors.join(" | ")}` },
        model,
        raw: text.slice(0, 2000),
      };
    }

    // Stable-ish session id derived from inputs (useful for provenance).
    const sessionId = `S-${sha256Hex(`${scope}\n${nowISO()}\n${userSessionText || ""}`).slice(0, 12)}`;
    return { ok: true, model, sessionId, ...v.normalized };
  } catch (err) {
    const e = classifyLlmError(err);
    return { ok: false, error: e, model };
  }
}
