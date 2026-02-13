import { existsSync } from "node:fs";
import { mkdir, readFile, rename, writeFile, appendFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";

import { validateDecisionPacket, validateKnowledgeEvent } from "../../contracts/validators/index.js";
import { stableId } from "./committee-utils.js";
import { renderDecisionPacketMd } from "./committee-utils.js";
import { ensureLaneADirs, loadProjectPaths } from "../../paths/project-paths.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

let atomicCounter = 0;
async function writeTextAtomic(absPath, text) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  atomicCounter += 1;
  const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
  await writeFile(tmp, String(text || ""), "utf8");
  await rename(tmp, abs);
}

function requireAbsProjectRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) throw new Error("Missing projectRoot.");
  if (!isAbsolute(raw)) throw new Error("projectRoot must be an absolute path.");
  return resolve(raw);
}

function nowISO() {
  return new Date().toISOString();
}

function normalizeDecisionIdArg(id) {
  const raw = normStr(id);
  if (!raw) return null;
  if (raw.startsWith("DECISION-")) return raw.slice("DECISION-".length);
  return raw;
}

function parseChoiceConstraints(constraints) {
  const s = String(constraints || "");
  const idx = s.toLowerCase().indexOf(":");
  const rhs = idx >= 0 ? s.slice(idx + 1) : s;
  return rhs
    .split("|")
    .map((x) => x.trim())
    .filter(Boolean);
}

function parseAnswerValue({ expectedType, rawText, constraints }) {
  const text = normStr(rawText);
  if (!text) throw new Error("Answer is empty.");

  if (expectedType === "boolean") {
    const lower = text.toLowerCase();
    if (lower === "true" || lower === "yes") return true;
    if (lower === "false" || lower === "no") return false;
    throw new Error("Invalid boolean answer. Expected: yes|no|true|false.");
  }

  if (expectedType === "choice") {
    const choices = parseChoiceConstraints(constraints);
    if (!choices.length) throw new Error("Invalid choice question: constraints does not define choices.");
    const match = choices.find((c) => c.toLowerCase() === text.toLowerCase());
    if (!match) throw new Error(`Invalid choice answer. Expected one of: ${choices.join(" | ")}`);
    return match;
  }

  if (expectedType === "reference") {
    if (!text.includes(":")) throw new Error("Invalid reference answer. Expected a reference like 'repo:<id>' or 'url:https://...'.");
    return text;
  }

  return text;
}

export async function answerDecisionPacket({ projectRoot, decisionId, inputPath, dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const id = normalizeDecisionIdArg(decisionId);
  if (!id) return { ok: false, message: "Missing --id <DECISION-id>." };
  const inputAbs = resolve(String(inputPath || ""));
  if (!existsSync(inputAbs)) return { ok: false, message: `Missing --input file (${inputAbs}).` };

  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  const knowledgeRootAbs = paths.knowledge.rootAbs;

  const decisionsDirAbs = paths.knowledge.decisionsAbs;
  const jsonAbs = join(decisionsDirAbs, `DECISION-${id}.json`);
  const mdAbs = join(decisionsDirAbs, `DECISION-${id}.md`);
  if (!existsSync(jsonAbs)) return { ok: false, message: `Decision not found: ${jsonAbs}` };

  const packet = JSON.parse(String(await readFile(jsonAbs, "utf8") || ""));
  validateDecisionPacket(packet);
  if (packet.status !== "open") return { ok: false, message: `Decision ${id} is not open (status=${packet.status}).` };

  const inputText = String(await readFile(inputAbs, "utf8") || "");
  const answersAt = nowISO();

  const updated = JSON.parse(JSON.stringify(packet));
  if (!Array.isArray(updated.questions) || updated.questions.length === 0) return { ok: false, message: "Decision has no questions." };

  if (updated.questions.length === 1) {
    const q = updated.questions[0];
    q.answer = parseAnswerValue({ expectedType: String(q.expected_answer_type), rawText: inputText, constraints: q.constraints });
    q.answered_at = answersAt;
  } else {
    let map = null;
    try {
      map = JSON.parse(inputText);
    } catch {
      map = null;
    }
    if (!map || typeof map !== "object" || Array.isArray(map)) return { ok: false, message: "Multiple questions require JSON object input mapping question_id -> answer." };
    for (let i = 0; i < updated.questions.length; i += 1) {
      const q = updated.questions[i];
      if (!(q.id in map)) return { ok: false, message: `Missing answer for question_id ${q.id}.` };
      q.answer = parseAnswerValue({ expectedType: String(q.expected_answer_type), rawText: String(map[q.id]), constraints: q.constraints });
      q.answered_at = answersAt;
    }
  }

  updated.status = "answered";
  updated.answered_at = answersAt;
  validateDecisionPacket(updated);

  const event = {
    event_id: stableId("EVT", ["decision_answered", updated.decision_id, updated.scope, answersAt]),
    type: "decision_answered",
    decision_id: updated.decision_id,
    scope: updated.scope,
    timestamp: answersAt,
  };
  validateKnowledgeEvent(event);

  if (!dryRun) {
    await mkdir(decisionsDirAbs, { recursive: true });
    await writeTextAtomic(jsonAbs, JSON.stringify(updated, null, 2) + "\n");
    await writeTextAtomic(mdAbs, renderDecisionPacketMd(updated));
    await appendFile(paths.laneA.ledgerAbs, JSON.stringify(event) + "\n", "utf8");
  }

  return {
    ok: true,
    decision_id: updated.decision_id,
    scope: updated.scope,
    status: updated.status,
    knowledge_root: knowledgeRootAbs,
    event,
    lane_a_ledger: paths.laneA.ledgerAbs,
  };
}
