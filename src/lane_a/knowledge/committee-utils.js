import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import { validateDecisionPacket } from "../../contracts/validators/index.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function renderCommitteeOutputBody(output) {
  const out = isPlainObject(output) ? output : {};
  const lines = [];

  lines.push("## Facts");
  lines.push("");
  const facts = Array.isArray(out.facts) ? out.facts : [];
  if (!facts.length) lines.push("- (none)");
  for (const f of facts) {
    const text = normStr(f?.text);
    const refs = Array.isArray(f?.evidence_refs) ? f.evidence_refs.map((x) => String(x || "").trim()).filter(Boolean) : [];
    if (!text) continue;
    lines.push(`- ${text}`);
    lines.push(`  - evidence_refs=[${refs.join(", ")}]`);
  }
  lines.push("");

  lines.push("## Assumptions");
  lines.push("");
  const assumptions = Array.isArray(out.assumptions) ? out.assumptions : [];
  if (!assumptions.length) lines.push("- (none)");
  for (const a of assumptions) {
    const text = normStr(a?.text);
    const miss = Array.isArray(a?.evidence_missing) ? a.evidence_missing.map((x) => String(x || "").trim()).filter(Boolean) : [];
    if (!text) continue;
    lines.push(`- ${text}`);
    if (miss.length) lines.push(`  - evidence_missing=[${miss.join(", ")}]`);
  }
  lines.push("");

  lines.push("## Unknowns");
  lines.push("");
  const unknowns = Array.isArray(out.unknowns) ? out.unknowns : [];
  if (!unknowns.length) lines.push("- (none)");
  for (const u of unknowns) {
    const text = normStr(u?.text);
    const miss = Array.isArray(u?.evidence_missing) ? u.evidence_missing.map((x) => String(x || "").trim()).filter(Boolean) : [];
    if (!text) continue;
    lines.push(`- ${text}`);
    if (miss.length) lines.push(`  - evidence_missing=[${miss.join(", ")}]`);
  }
  lines.push("");

  lines.push("## Integration edges");
  lines.push("");
  const edges = Array.isArray(out.integration_edges) ? out.integration_edges : [];
  if (!edges.length) lines.push("- (none)");
  for (const e of edges) {
    const from = normStr(e?.from);
    const to = normStr(e?.to);
    const type = normStr(e?.type);
    const contract = normStr(e?.contract);
    const conf = typeof e?.confidence === "number" && Number.isFinite(e.confidence) ? e.confidence : null;
    const refs = Array.isArray(e?.evidence_refs) ? e.evidence_refs.map((x) => String(x || "").trim()).filter(Boolean) : [];
    const miss = Array.isArray(e?.evidence_missing) ? e.evidence_missing.map((x) => String(x || "").trim()).filter(Boolean) : [];
    lines.push(`- ${from} -> ${to} (${type}) confidence=${conf ?? "-"}`);
    if (contract) lines.push(`  - contract: ${contract}`);
    if (refs.length) lines.push(`  - evidence_refs=[${refs.join(", ")}]`);
    if (miss.length) lines.push(`  - evidence_missing=[${miss.join(", ")}]`);
  }
  lines.push("");

  lines.push("## Risks");
  lines.push("");
  const risks = Array.isArray(out.risks) ? out.risks : [];
  if (!risks.length) lines.push("- (none)");
  for (const r of risks) {
    const s = normStr(r);
    if (s) lines.push(`- ${s}`);
  }
  lines.push("");

  lines.push("## Verdict");
  lines.push("");
  lines.push(normStr(out.verdict) || "-");
  lines.push("");

  return lines;
}

export function stableId(prefix, parts) {
  const base = (Array.isArray(parts) ? parts : []).map((p) => String(p ?? "")).join("\n");
  const h = createHash("sha256").update(base, "utf8").digest("hex").slice(0, 12);
  return `${prefix}_${h}`;
}

let atomicCounter = 0;
export async function writeTextAtomic(absPath, text) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  atomicCounter += 1;
  const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
  await writeFile(tmp, String(text || ""), "utf8");
  await rename(tmp, abs);
}

export async function readJsonAbs(pathAbs) {
  const abs = resolve(String(pathAbs || ""));
  const t = await readFile(abs, "utf8");
  return JSON.parse(String(t || ""));
}

export function clampInt(n, { min = 0, max = 999 } = {}) {
  const v = Number.isFinite(Number(n)) ? Math.floor(Number(n)) : min;
  return Math.max(min, Math.min(max, v));
}

export function normalizeConfidenceToken(x) {
  const s = normStr(x).toLowerCase();
  if (s === "high" || s === "medium" || s === "low") return s;
  return "low";
}

export function confidenceTokenToNumber(token) {
  const t = normalizeConfidenceToken(token);
  if (t === "high") return 0.85;
  if (t === "medium") return 0.6;
  return 0.35;
}

export function renderClaimsMd({ repo_id, created_at, claims, role }) {
  const lines = [];
  lines.push(`# Committee: ${repo_id} (${role})`);
  lines.push("");
  lines.push(`created_at: ${created_at}`);
  lines.push("");
  lines.push(...renderCommitteeOutputBody(claims));
  return lines.join("\n") + "\n";
}

export function renderChallengesMd({ repo_id, created_at, challenges }) {
  const lines = [];
  lines.push(`# Committee: ${repo_id} (repo_skeptic)`);
  lines.push("");
  lines.push(`created_at: ${created_at}`);
  lines.push("");
  lines.push(...renderCommitteeOutputBody(challenges));
  return lines.join("\n") + "\n";
}

export function renderIntegrationMd({ created_at, gaps }) {
  const lines = [];
  lines.push("# Committee: integration_chair");
  lines.push("");
  lines.push(`created_at: ${created_at}`);
  lines.push("");
  lines.push(...renderCommitteeOutputBody(gaps));
  return lines.join("\n") + "\n";
}

export function renderDecisionPacketMd(packet) {
  const p = packet;
  const lines = [];
  lines.push(`# Decision Packet: ${p.decision_id}`);
  lines.push("");
  lines.push(`status: ${p.status}`);
  if (typeof p.type === "string" && p.type.trim()) lines.push(`type: ${p.type}`);
  lines.push(`scope: ${p.scope}`);
  lines.push(`trigger: ${p.trigger}`);
  lines.push(`blocking_state: ${p.blocking_state}`);
  lines.push("");
  lines.push("## Context");
  lines.push("");
  lines.push(p.context.summary);
  lines.push("");
  lines.push("Why this is being asked:");
  lines.push(p.context.why_automation_failed);
  lines.push("");
  lines.push("What is known:");
  lines.push("");
  if (!p.context.what_is_known.length) lines.push("- (none)");
  for (const k of p.context.what_is_known) lines.push(`- ${k}`);
  lines.push("");
  lines.push("## Questions");
  lines.push("");
  for (const q of p.questions) {
    lines.push(`- ${q.id} (${q.expected_answer_type})`);
    lines.push(`  - ${q.question}`);
    if (q.constraints) lines.push(`  - constraints: ${q.constraints}`);
    if (Array.isArray(q.blocks) && q.blocks.length) lines.push(`  - blocks: ${q.blocks.join(", ")}`);
    if (q.answer !== undefined) lines.push(`  - answer: ${String(q.answer)}`);
  }
  lines.push("");
  lines.push("## Assumptions if unanswered");
  lines.push("");
  lines.push(p.assumptions_if_unanswered);
  lines.push("");
  return lines.join("\n") + "\n";
}

export function buildDecisionPacket({
  scope,
  trigger,
  blocking_state,
  context_summary,
  why_automation_failed,
  what_is_known,
  question,
  expected_answer_type,
  constraints,
  blocks,
  assumptions_if_unanswered,
  created_at,
}) {
  const scopeNorm = normStr(scope);
  const trigNorm = normStr(trigger);
  const blockNorm = normStr(blocking_state);
  const qNorm = normStr(question);
  const decision_id = stableId("DEC", [scopeNorm, trigNorm, blockNorm, qNorm, normStr(constraints)]).slice("DEC_".length);
  const q_id = stableId("Q", [decision_id, qNorm]).slice("Q_".length);

  const packet = {
    version: 1,
    decision_id,
    scope: scopeNorm,
    trigger: trigNorm,
    blocking_state: blockNorm,
    context: {
      summary: normStr(context_summary),
      why_automation_failed: normStr(why_automation_failed),
      what_is_known: Array.from(new Set((Array.isArray(what_is_known) ? what_is_known : []).map((x) => normStr(x)).filter(Boolean))).sort((a, b) => a.localeCompare(b)),
    },
    questions: [
      {
        id: q_id,
        question: qNorm,
        expected_answer_type: normStr(expected_answer_type),
        constraints: typeof constraints === "string" ? constraints : "",
        blocks: Array.from(new Set((Array.isArray(blocks) ? blocks : []).map((x) => normStr(x)).filter(Boolean))).sort((a, b) => a.localeCompare(b)),
      },
    ],
    assumptions_if_unanswered: normStr(assumptions_if_unanswered),
    created_at: normStr(created_at),
    status: "open",
  };
  validateDecisionPacket(packet);
  return packet;
}

export function assertJsonObject(x, name) {
  if (!isPlainObject(x)) throw new Error(`${name} must be an object.`);
  return x;
}
