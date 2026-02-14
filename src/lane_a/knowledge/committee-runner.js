import { existsSync, readFileSync } from "node:fs";
import { mkdir, readdir, readFile, rm } from "node:fs/promises";
import { cpus } from "node:os";
import { basename, isAbsolute, join, resolve } from "node:path";

import { createLlmClient } from "../../llm/client.js";
import { maybeAugmentLlmMessagesWithSkills } from "../../llm/prompt-augment.js";
import { loadLlmProfiles, resolveLlmProfileOrError } from "../../llm/llm-profiles.js";
import { loadProjectPaths } from "../../paths/project-paths.js";
import { loadRepoRegistry, resolveRepoAbsPath } from "../../utils/repo-registry.js";
import { gitShowFileAtRef } from "../../utils/git-files.js";
import { runWorkerPool } from "../../utils/pool.js";
import { evaluateRepoStaleness, evaluateScopeStaleness, writeRefreshRequiredDecisionPacketIfNeeded } from "../lane-a-staleness-policy.js";
import { readPhaseStateOrDefault } from "../phase-state.js";
import { appendFile } from "../../utils/fs.js";
import { nowFsSafeUtcTimestamp } from "../../utils/naming.js";
import {
  buildSystemSoftStaleSnapshotFromRepoInfos,
  maybePrependSoftStaleBanner,
  recordSoftStaleObservation,
  selectSoftStaleBannerRepoSnapshot,
} from "../staleness/soft-stale-escalation.js";
import {
  validateCommitteeOutput,
  validateCommitteeStatus,
  validateDecisionPacket,
  validateEvidenceRef,
  validateIntegrationStatus,
  validateQaCommitteeOutput,
  validateRepoIndex,
} from "../../contracts/validators/index.js";
import { validateScope } from "./knowledge-utils.js";
import { assertKickoffLatestShape, readJsonAbs as readJsonAbsKickoff } from "./kickoff-utils.js";
import { buildDecisionPacket, clampInt, normalizeConfidenceToken, stableId, writeTextAtomic, readJsonAbs, renderClaimsMd, renderChallengesMd, renderDecisionPacketMd, renderIntegrationMd } from "./committee-utils.js";
import { readSufficiencyOrDefault } from "./knowledge-sufficiency.js";

function nowISO() {
  return new Date().toISOString();
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function scopeToDirName(parsedScope) {
  if (!parsedScope || typeof parsedScope !== "object") return "unknown_scope";
  if (parsedScope.kind === "system") return "system";
  if (parsedScope.kind === "repo") return `repo_${String(parsedScope.repo_id || "unknown")}`;
  return "unknown_scope";
}

async function buildAugmentedCommitteeMessages({
  paths,
  scope,
  systemPrompt,
  userPrompt,
  context = null,
} = {}) {
  const userText = typeof userPrompt === "string" ? userPrompt : JSON.stringify(userPrompt || {});
  const baseMessages = [
    { role: "system", content: String(systemPrompt || "") },
    { role: "user", content: userText },
  ];
  const augmented = await maybeAugmentLlmMessagesWithSkills({
    baseMessages,
    projectRoot: paths?.opsRootAbs || null,
    input: {
      scope,
      base_system: String(systemPrompt || ""),
      base_prompt: userText,
      context: context && typeof context === "object" ? context : {},
      constraints: { output: "json_only", role: "committee" },
      knowledge_snippets: [],
    },
  });
  return augmented.messages;
}

async function resolveLaneAPhaseForCommittee({ paths }) {
  const r = await readPhaseStateOrDefault({ projectRoot: paths.opsRootAbs });
  const p = r.phase;
  if (p && p.forward && String(p.forward.status || "").trim() === "in_progress") return "forward";
  if (p && p.reverse && String(p.reverse.status || "").trim() !== "closed") return "reverse";
  return "none";
}

async function readSsotSectionContentOptional({ paths, sectionId }) {
  const id = normStr(sectionId);
  if (!id) return "";
  const abs = join(paths.knowledge.ssotSystemAbs, "sections", `${id}.json`);
  if (!existsSync(abs)) return "";
  try {
    const j = JSON.parse(String(readFileSync(abs, "utf8") || ""));
    const c = typeof j?.content === "string" ? j.content : "";
    return String(c || "").trim();
  } catch {
    return "";
  }
}

async function computeChallengeQuestion({ paths }) {
  // Deterministic SDLC ladder: Vision → Requirements → Constraints → Domain Data → API → Infra → Ops.
  // Use SSOT system section content presence as a simple, deterministic gate.
  const vision = await readSsotSectionContentOptional({ paths, sectionId: "vision" });
  if (!vision) {
    return { stage: "VISION", question: "What is the system/product vision? (one paragraph)", why: "Vision is required before requirements and delivery work can be scoped safely." };
  }
  const scope = await readSsotSectionContentOptional({ paths, sectionId: "scope" });
  if (!scope) {
    return { stage: "REQUIREMENTS", question: "What are the key business flows/actors and what is explicitly in-scope vs out-of-scope?", why: "Requirements and scope must be clear before domain data or API questions." };
  }
  const constraints = await readSsotSectionContentOptional({ paths, sectionId: "constraints" });
  if (!constraints) {
    return { stage: "CONSTRAINTS", question: "List the top constraints (technical/regulatory/time/budget/compat) that must be respected.", why: "Constraints affect design and feasibility of delivery work." };
  }
  return { stage: "DOMAIN_DATA", question: "Name the core domain entities and invariants (ownership, lifecycle, key states).", why: "Domain data must be explicit before API/contract questions." };
}

function requireAbsProjectRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) return null;
  if (!isAbsolute(raw)) throw new Error("--projectRoot must be an absolute path (OPS_ROOT).");
  return resolve(raw);
}

async function readJsonFile(absPath) {
  const t = readFileSync(resolve(absPath), "utf8");
  return JSON.parse(String(t || ""));
}

function knowledgeReadPathsFromBundle({ laneARootAbs, parsedScope, bundleId }) {
  const bid = normStr(bundleId);
  if (!bid) return null;
  if (!parsedScope || parsedScope.kind !== "repo" || !parsedScope.repo_id) {
    throw new Error("--bundle-id is only supported for repo:<repo_id> committee runs.");
  }
  const contentAbs = resolve(String(laneARootAbs || ""), "bundles", "repo", parsedScope.repo_id, bid, "content");
  return {
    rootAbs: contentAbs,
    ssotReposAbs: join(contentAbs, "ssot", "repos"),
    ssotSystemAbs: join(contentAbs, "ssot", "system"),
    evidenceIndexReposAbs: join(contentAbs, "evidence", "index", "repos"),
    evidenceReposAbs: join(contentAbs, "evidence", "repos"),
    decisionsAbs: join(contentAbs, "decisions"),
    viewsReposAbs: join(contentAbs, "views", "repos"),
  };
}

function knowledgeReadSystemPathsFromBundle({ laneARootAbs, bundleId }) {
  const bid = normStr(bundleId);
  if (!bid) return null;
  const contentAbs = resolve(String(laneARootAbs || ""), "bundles", "system", bid, "content");
  return {
    rootAbs: contentAbs,
    integrationMapAbs: join(contentAbs, "views", "integration_map.json"),
  };
}

function readLatestBundleIdForScope({ laneARootAbs, scope }) {
  const abs = resolve(String(laneARootAbs || ""), "bundles", "LATEST.json");
  if (!existsSync(abs)) return null;
  try {
    const j = JSON.parse(String(readFileSync(abs, "utf8") || ""));
    const entry = j && j.latest_by_scope && typeof j.latest_by_scope === "object" ? j.latest_by_scope[String(scope)] : null;
    const bid = entry && typeof entry.bundle_id === "string" ? entry.bundle_id.trim() : "";
    return bid || null;
  } catch {
    return null;
  }
}

function listActiveRepoIds(reposJson) {
  const repos = Array.isArray(reposJson?.repos) ? reposJson.repos : [];
  return repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => normStr(r?.repo_id))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

function resolveRepoAbs({ reposJson, repoId }) {
  const baseDir = normStr(reposJson?.base_dir);
  if (!baseDir) throw new Error("REPOS_ROOT missing (config/PROJECT.json.repos_root_abs).");
  if (!isAbsolute(baseDir)) throw new Error("REPOS_ROOT must be absolute.");

  const repos = Array.isArray(reposJson?.repos) ? reposJson.repos : [];
  const found = repos.find((r) => normStr(r?.repo_id) === repoId);
  if (!found) throw new Error(`Unknown repo_id: ${repoId}`);
  const relPath = normStr(found?.path);
  if (!relPath) throw new Error(`Repo ${repoId} missing path.`);
  return resolveRepoAbsPath({ baseDir, repoPath: relPath });
}

function loadEvidenceRefsJsonl(absPath) {
  const text = String(readFileSync(absPath, "utf8") || "");
  const lines = text.split("\n").map((l) => l.trim()).filter(Boolean);
  const refs = [];
  for (let i = 0; i < lines.length; i += 1) {
    const obj = JSON.parse(lines[i]);
    validateEvidenceRef(obj);
    refs.push(obj);
  }
  refs.sort((a, b) => String(a.file_path).localeCompare(String(b.file_path)));
  return refs;
}

function sliceLines(text, startLine, endLine) {
  const start = Math.max(1, Number.isFinite(Number(startLine)) ? Math.floor(Number(startLine)) : 1);
  const end = Math.max(start, Number.isFinite(Number(endLine)) ? Math.floor(Number(endLine)) : start);
  const lines = String(text || "").split("\n");
  const sIdx = Math.min(lines.length, Math.max(1, start)) - 1;
  const eIdx = Math.min(lines.length, Math.max(1, end));
  return lines.slice(sIdx, eIdx).join("\n").trimEnd();
}

function buildEvidenceBundle({ repoAbs, refs }) {
  const out = [];
  for (const r of refs) {
    const shown = gitShowFileAtRef(repoAbs, r.commit_sha, r.file_path);
    if (!shown.ok) throw new Error(`git show failed for ${r.commit_sha}:${r.file_path} (${shown.error})`);
    const excerpt = sliceLines(shown.content, r.start_line, r.end_line);
    out.push({
      evidence_id: r.evidence_id,
      file_path: r.file_path,
      commit_sha: r.commit_sha,
      start_line: r.start_line,
      end_line: r.end_line,
      excerpt,
    });
  }
  return out;
}

function parseJsonFromLlmContent(content) {
  const raw = typeof content === "string" ? content : String(content ?? "");
  const trimmed = raw.trim();
  return JSON.parse(trimmed);
}

function readCommitteePrompt(relPathFromRepoRoot) {
  const abs = resolve(relPathFromRepoRoot);
  return readFileSync(abs, "utf8");
}

function readQaStrategistPrompt() {
  const abs = resolve("src/llm/prompts/committee/qa-strategist.system.txt");
  if (!existsSync(abs)) throw new Error("Missing QA strategist prompt. Expected src/llm/prompts/committee/qa-strategist.system.txt.");
  return readFileSync(abs, "utf8");
}

function normalizeEvidenceRefsList(rawList) {
  const list = Array.isArray(rawList) ? rawList : [];
  const out = [];
  for (const x of list) {
    const s = normStr(x);
    if (!s) continue;
    out.push(s);
  }
  return Array.from(new Set(out)).sort((a, b) => a.localeCompare(b));
}

function normalizeEvidenceMissingList(rawList) {
  const list = Array.isArray(rawList) ? rawList : [];
  const out = [];
  for (const x of list) {
    const s = normStr(x);
    if (!s) continue;
    // Back-compat normalization: convert legacy ID-ish entries into actionable descriptions.
    if (/^(EVID|CLAIM|CHAL|GAP|DEC|Q|FACT)_[A-Za-z0-9]+$/.test(s)) {
      out.push(`need evidence: file: (locate source for referenced id ${s})`);
      continue;
    }
    if (/^[A-Z][A-Z0-9_]{2,}:.+/.test(s) && !/^(file|path|endpoint):/i.test(s)) {
      out.push(`need evidence: file: (resolve referenced token ${s})`);
      continue;
    }
    if (/^[a-f0-9]{16,}$/i.test(s)) {
      out.push(`need evidence: file: (resolve referenced hash ${s})`);
      continue;
    }
    out.push(s);
  }
  return Array.from(new Set(out)).sort((a, b) => a.localeCompare(b));
}

function capCommitteeOutput(raw) {
  const out = raw;
  out.facts = (Array.isArray(out.facts) ? out.facts : []).slice().sort((a, b) => String(a.text).localeCompare(String(b.text))).slice(0, 20);
  out.assumptions = (Array.isArray(out.assumptions) ? out.assumptions : []).slice().sort((a, b) => String(a.text).localeCompare(String(b.text))).slice(0, 20);
  out.unknowns = (Array.isArray(out.unknowns) ? out.unknowns : []).slice().sort((a, b) => String(a.text).localeCompare(String(b.text))).slice(0, 20);
  out.integration_edges = (Array.isArray(out.integration_edges) ? out.integration_edges : [])
    .slice()
    .sort((a, b) => `${a.from}::${a.to}::${a.type}::${a.contract}`.localeCompare(`${b.from}::${b.to}::${b.type}::${b.contract}`))
    .slice(0, 20);
  out.risks = (Array.isArray(out.risks) ? out.risks : []).slice().sort((a, b) => String(a).localeCompare(String(b))).slice(0, 20);

  for (const f of out.facts) f.evidence_refs = normalizeEvidenceRefsList(f.evidence_refs);
  for (const e of out.integration_edges) {
    e.evidence_refs = normalizeEvidenceRefsList(e.evidence_refs);
    e.evidence_missing = normalizeEvidenceMissingList(e.evidence_missing);
  }
  for (const a of out.assumptions) a.evidence_missing = normalizeEvidenceMissingList(a.evidence_missing);
  for (const u of out.unknowns) u.evidence_missing = normalizeEvidenceMissingList(u.evidence_missing);
  return out;
}

function applySoftStaleMarker(out, staleInfo) {
  const msg = `need refresh required: file: run --knowledge-refresh-from-events and/or --knowledge-index/--knowledge-scan before committee (reason=${normStr(staleInfo?.stale_reason) || "stale"})`;
  const next = { ...out, stale: true };
  const unknowns = Array.isArray(next.unknowns) ? next.unknowns.slice() : [];
  unknowns.push({
    text: "Repo appears stale relative to scan and/or merge events; committee output is degraded until refreshed.",
    evidence_missing: [msg],
  });
  next.unknowns = unknowns;
  return next;
}

function applySoftStaleMarkerSystem(out, staleRepoInfos) {
  const infos = Array.isArray(staleRepoInfos) ? staleRepoInfos : [];
  const parts = infos
    .map((i) => {
      const id = normStr(i?.repo_id);
      const reason = normStr(i?.stale_reason) || "stale";
      return id ? `${id}:${reason}` : null;
    })
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
  const msg = `need refresh required: file: run --knowledge-refresh-from-events and/or --knowledge-index/--knowledge-scan before integration chair (stale_repos=${parts.join(",") || "unknown"})`;
  const next = { ...out, stale: true };
  const unknowns = Array.isArray(next.unknowns) ? next.unknowns.slice() : [];
  unknowns.push({
    text: "One or more repos appear stale relative to scan and/or merge events; integration output is degraded until refreshed.",
    evidence_missing: [msg],
  });
  next.unknowns = unknowns;
  return next;
}

function repoScopeSnapshotFromRepoStaleInfo(staleInfo, repoId) {
  const id = normStr(repoId) || normStr(staleInfo?.repo_id);
  return {
    scope: id ? `repo:${id}` : "repo:unknown",
    stale: staleInfo?.stale === true,
    hard_stale: staleInfo?.hard_stale === true,
    reasons: Array.isArray(staleInfo?.stale_reasons) ? staleInfo.stale_reasons.slice().sort((a, b) => a.localeCompare(b)) : [],
    stale_repos: staleInfo?.stale === true && id ? [id] : [],
    hard_stale_repos: staleInfo?.hard_stale === true && id ? [id] : [],
    repo_id: id || null,
    repo_head_sha: staleInfo?.repo_head_sha || null,
    last_scanned_head_sha: staleInfo?.last_scanned_head_sha || null,
    last_scan_time: staleInfo?.last_scan_time || null,
    last_merge_event_time: staleInfo?.last_merge_event_time || null,
  };
}

function applySoftStaleStatusMeta(status, stalenessSnapshot) {
  const next = { ...status };
  if (!(stalenessSnapshot?.stale === true) || stalenessSnapshot?.hard_stale === true) return next;
  next.degraded = true;
  next.degraded_reason = "soft_stale";
  next.stale = true;
  next.hard_stale = false;
  next.staleness = stalenessSnapshot;
  return next;
}

function validateEvidenceRefsMembership({ output, allowedEvidenceIds }) {
  const unknown = [];
  const check = (id, path) => {
    if (!allowedEvidenceIds.has(id)) unknown.push({ id, path });
  };
  for (let i = 0; i < output.facts.length; i += 1) {
    for (let j = 0; j < output.facts[i].evidence_refs.length; j += 1) check(output.facts[i].evidence_refs[j], `facts[${i}].evidence_refs[${j}]`);
  }
  for (let i = 0; i < output.integration_edges.length; i += 1) {
    for (let j = 0; j < output.integration_edges[i].evidence_refs.length; j += 1) check(output.integration_edges[i].evidence_refs[j], `integration_edges[${i}].evidence_refs[${j}]`);
  }
  if (unknown.length) {
    const first = unknown[0];
    throw new Error(`committee output references unknown evidence_ref ${first.id} at ${first.path}`);
  }
}

function parseAndValidateCommitteeOutput({ rawText, expectedScope, allowedEvidenceIds }) {
  const raw = parseJsonFromLlmContent(rawText);
  validateCommitteeOutput(raw);
  if (String(raw.scope) !== String(expectedScope)) throw new Error(`committee output scope mismatch: expected ${expectedScope} got ${String(raw.scope)}`);
  const capped = capCommitteeOutput(raw);
  validateCommitteeOutput(capped);
  validateEvidenceRefsMembership({ output: capped, allowedEvidenceIds });
  return capped;
}

function parseAndValidateQaCommitteeOutput({ rawText, expectedScope }) {
  const raw = parseJsonFromLlmContent(rawText);
  validateQaCommitteeOutput(raw);
  if (String(raw.scope) !== String(expectedScope)) throw new Error(`qa_strategist output scope mismatch: expected ${expectedScope} got ${String(raw.scope)}`);
  return raw;
}

function renderQaStrategistMd(out) {
  const lines = [];
  lines.push(`# QA Strategist (Lane A committee)`);
  lines.push("");
  lines.push(`- scope: \`${out.scope}\``);
  lines.push(`- created_at: \`${out.created_at}\``);
  lines.push(`- risk: \`${out.risk?.level || "unknown"}\``);
  lines.push("");
  lines.push(`## Required invariants`);
  lines.push("");
  const invs = Array.isArray(out.required_invariants) ? out.required_invariants : [];
  if (!invs.length) lines.push("- (none)");
  for (const inv of invs) {
    lines.push(`- [${inv.severity}] ${String(inv.text || "").trim()}`);
    const refs = Array.isArray(inv.evidence_refs) ? inv.evidence_refs : [];
    const miss = Array.isArray(inv.evidence_missing) ? inv.evidence_missing : [];
    if (refs.length) lines.push(`  - evidence_refs: ${refs.map((r) => `\`${r}\``).join(", ")}`);
    if (miss.length) lines.push(`  - evidence_missing: ${miss.map((m) => `\`${m}\``).join(" | ")}`);
  }
  lines.push("");
  lines.push(`## Test obligations`);
  lines.push("");
  const o = out.test_obligations || {};
  for (const k of ["unit", "integration", "e2e"]) {
    const ob = o && typeof o === "object" ? o[k] : null;
    lines.push(`### ${k}`);
    lines.push("");
    lines.push(`- required: \`${ob?.required ? "true" : "false"}\``);
    if (typeof ob?.why === "string" && ob.why.trim()) lines.push(`- why: ${ob.why.trim()}`);
    const dirs = Array.isArray(ob?.suggested_test_directives) ? ob.suggested_test_directives : [];
    if (dirs.length) lines.push(`- directives: ${dirs.map((d) => `\`${d}\``).join(", ")}`);
    const targets = Array.isArray(ob?.target_paths) ? ob.target_paths : [];
    if (targets.length) lines.push(`- targets: ${targets.map((p) => `\`${p}\``).join(", ")}`);
    lines.push("");
  }
  lines.push(`## Evidence-backed facts`);
  lines.push("");
  const facts = Array.isArray(out.facts) ? out.facts : [];
  if (!facts.length) lines.push("- (none)");
  for (const f of facts) lines.push(`- ${String(f.text || "").trim()}`);
  lines.push("");
  lines.push(`## Unknowns`);
  lines.push("");
  const unknowns = Array.isArray(out.unknowns) ? out.unknowns : [];
  if (!unknowns.length) lines.push("- (none)");
  for (const u of unknowns) lines.push(`- ${String(u.text || "").trim()}`);
  lines.push("");
  return lines.join("\n");
}

function collectEvidenceMissingFromOutput(out) {
  const missing = [];
  for (const a of Array.isArray(out.assumptions) ? out.assumptions : []) missing.push(...(Array.isArray(a.evidence_missing) ? a.evidence_missing : []));
  for (const u of Array.isArray(out.unknowns) ? out.unknowns : []) missing.push(...(Array.isArray(u.evidence_missing) ? u.evidence_missing : []));
  for (const e of Array.isArray(out.integration_edges) ? out.integration_edges : []) missing.push(...(Array.isArray(e.evidence_missing) ? e.evidence_missing : []));
  return Array.from(new Set(missing.map((x) => normStr(x)).filter(Boolean))).sort((a, b) => a.localeCompare(b));
}

function collectEvidenceRefsFromOutput(out) {
  const refs = new Set();
  for (const f of Array.isArray(out.facts) ? out.facts : []) for (const e of Array.isArray(f.evidence_refs) ? f.evidence_refs : []) refs.add(String(e));
  for (const ed of Array.isArray(out.integration_edges) ? out.integration_edges : []) for (const e of Array.isArray(ed.evidence_refs) ? ed.evidence_refs : []) refs.add(String(e));
  return refs;
}

function deriveCommitteeStatusFromOutputs({ repoId, architect, skeptic }) {
  const blocking = [];
  const add = (description, severity, evidence_missing) => {
    const desc = normStr(description);
    const sev = severity === "high" || severity === "medium" || severity === "low" ? severity : "high";
    const miss = normalizeEvidenceMissingList(evidence_missing);
    blocking.push({
      id: stableId("ISSUE", [repoId, desc, sev, miss.join(",")]),
      description: desc,
      evidence_missing: miss,
      severity: sev,
    });
  };

  const missing = []
    .concat(collectEvidenceMissingFromOutput(architect))
    .concat(collectEvidenceMissingFromOutput(skeptic));

  for (const m of Array.from(new Set(missing)).sort((a, b) => a.localeCompare(b))) add(`Missing evidence: ${m}`, "medium", [m]);
  if (architect.verdict !== "evidence_valid") add("repo_architect verdict is evidence_invalid", "high", []);
  if (skeptic.verdict !== "evidence_valid") add("repo_skeptic verdict is evidence_invalid", "high", []);

  blocking.sort((a, b) => a.id.localeCompare(b.id));
  const hasHigh = blocking.some((b) => b.severity === "high");
  const hasMissing = blocking.some((b) => b.severity === "medium" && b.evidence_missing.length > 0);
  const evidence_valid = !(hasHigh || hasMissing);
  const next_action = hasMissing ? "rescan_needed" : hasHigh ? "decision_needed" : "proceed";
  const confidence = evidence_valid ? "high" : hasMissing ? "medium" : "low";

  const status = { version: 1, repo_id: repoId, evidence_valid, blocking_issues: blocking, confidence, next_action };
  validateCommitteeStatus(status);
  return status;
}

async function loadKickoffInputs({ knowledgeRootAbs, scope }) {
  const latestAbs = join(knowledgeRootAbs, "sessions", "kickoff", "LATEST.json");
  if (!existsSync(latestAbs)) return { ok: true, system: null, repo: null };
  const latest = assertKickoffLatestShape(await readJsonAbsKickoff(latestAbs));
  const sys = latest.latest_by_scope?.system || null;
  const rep = latest.latest_by_scope?.[scope] || null;
  const loadJsonIf = async (summary) => {
    if (!summary) return null;
    const p = join(knowledgeRootAbs, "sessions", "kickoff", String(summary.latest_json));
    if (!existsSync(p)) return null;
    // eslint-disable-next-line no-await-in-loop
    return readJsonAbsKickoff(p);
  };
  return { ok: true, system: await loadJsonIf(sys), repo: await loadJsonIf(rep) };
}

async function loadAnsweredDecisions({ decisionsDirAbs, scopes }) {
  const dirAbs = resolve(String(decisionsDirAbs || ""));
  if (!existsSync(dirAbs)) return [];
  const scopeSet = new Set((Array.isArray(scopes) ? scopes : []).map((s) => normStr(s)).filter(Boolean));
  if (!scopeSet.size) return [];
  const entries = await readdir(dirAbs, { withFileTypes: true });
  const files = entries
    .filter((e) => e.isFile() && e.name.startsWith("DECISION-") && e.name.endsWith(".json"))
    .map((e) => join(dirAbs, e.name))
    .sort((a, b) => a.localeCompare(b));

  const out = [];
  for (const abs of files) {
    // eslint-disable-next-line no-await-in-loop
    const j = await readJsonAbs(abs);
    validateDecisionPacket(j);
    if (j.status !== "answered") continue;
    if (!scopeSet.has(String(j.scope))) continue;
    out.push({
      decision_id: String(j.decision_id),
      scope: String(j.scope),
      trigger: String(j.trigger),
      blocking_state: String(j.blocking_state),
      questions: Array.isArray(j.questions)
        ? j.questions.map((q) => ({ id: String(q.id), answer: q.answer, expected_answer_type: String(q.expected_answer_type) }))
        : [],
      answered_at: String(j.answered_at || ""),
    });
  }
  out.sort((a, b) => a.decision_id.localeCompare(b.decision_id));
  return out;
}

async function writeDecisionPackets({ decisionsDirAbs, repoId, blockers, evidenceFallbackIds, dryRun }) {
  const dir = resolve(String(decisionsDirAbs || ""));
  const written = [];
  for (const b of blockers) {
    const packet = buildDecisionPacket({
      scope: repoId ? `repo:${repoId}` : "system",
      trigger: repoId ? "repo_committee" : "integration_committee",
      blocking_state: repoId ? "COMMITTEE_REPO_FAILED" : "COMMITTEE_INTEGRATION_FAILED",
      context_summary: repoId ? `Committee found a blocking issue in repo ${repoId}.` : "Integration chair found a blocking integration issue.",
      why_automation_failed: "Lane A cannot proceed safely until a human clarifies the decision boundary for this blocker.",
      what_is_known: []
        .concat(b && b.id ? [String(b.id)] : [])
        .concat(Array.isArray(b.evidence_refs) ? b.evidence_refs : [])
        .concat(Array.isArray(evidenceFallbackIds) ? evidenceFallbackIds : [])
        .slice(0, 20),
      question: `Which option should Lane A adopt to resolve this blocker: ${String(b.description || "").trim()}`,
      expected_answer_type: "choice",
      constraints: "Choose one: keep|discard|rescan_needed",
      blocks: ["COMMITTEE_PENDING"],
      assumptions_if_unanswered: "Default to 'rescan_needed' and block progression.",
      created_at: nowISO(),
    });
    validateDecisionPacket(packet);
    const stem = `DECISION-${packet.decision_id}`;
    const jsonAbs = join(dir, `${stem}.json`);
    const mdAbs = join(dir, `${stem}.md`);
    if (!dryRun) {
      await mkdir(dir, { recursive: true });
      if (!existsSync(jsonAbs)) await writeTextAtomic(jsonAbs, JSON.stringify(packet, null, 2) + "\n");
      if (!existsSync(mdAbs)) await writeTextAtomic(mdAbs, renderDecisionPacketMd(packet));
    }
    written.push({ decision_id: packet.decision_id, json: `decisions/${basename(jsonAbs)}`, md: `decisions/${basename(mdAbs)}` });
  }
  return written;
}

async function runRepoCommittee({ paths, reposJson, repoId, dryRun, bundleId = null, knowledgeRead = null, forceStaleOverride = false }) {
  const readPaths = knowledgeRead || paths.knowledge;
  const idxAbs = join(readPaths.evidenceIndexReposAbs, repoId, "repo_index.json");
  const refsAbs = join(readPaths.evidenceReposAbs, repoId, "evidence_refs.jsonl");
  if (!existsSync(idxAbs)) throw new Error(`Missing repo_index.json for ${repoId} at ${idxAbs}. Run --knowledge-index.`);
  if (!existsSync(refsAbs)) throw new Error(`Missing evidence_refs.jsonl for ${repoId} at ${refsAbs}. Run --knowledge-scan.`);

  const repoIndex = await readJsonAbs(idxAbs);
  validateRepoIndex(repoIndex);
  const refs = loadEvidenceRefsJsonl(refsAbs);
  const evidenceIds = new Set(refs.map((r) => r.evidence_id));
  if (!evidenceIds.size) throw new Error(`No evidence refs found for ${repoId}.`);

  const stalenessPaths = { ...paths, knowledge: { ...paths.knowledge, rootAbs: readPaths.rootAbs } };
  const staleInfo = await evaluateRepoStaleness({ paths: stalenessPaths, registry: reposJson, repoId });
  const repoScopeStaleness = repoScopeSnapshotFromRepoStaleInfo(staleInfo, repoId);
  await recordSoftStaleObservation({
    paths,
    scope: `repo:${repoId}`,
    stalenessSnapshot: repoScopeStaleness,
  });
  if (staleInfo.hard_stale && !forceStaleOverride) {
    const decision = await writeRefreshRequiredDecisionPacketIfNeeded({
      paths,
      repoId,
      blockingState: "COMMITTEE_PENDING",
      staleInfo,
      producer: "committee",
      dryRun,
    });
    return {
      ok: false,
      repo_id: repoId,
      evidence_valid: false,
      reason_code: "STALE_BLOCKED",
      message: `STALE_BLOCKED: ${normStr(staleInfo.stale_reason) || "stale"}`,
      stale: true,
      stale_reason: staleInfo.stale_reason,
      hard_stale: true,
      decisions_written: decision?.json_abs ? [decision.json_abs] : [],
    };
  }

  let evidence = null;
  const bid = normStr(bundleId);
  if (bid) {
    const bundleEvidenceAbs = join(readPaths.rootAbs, "bundle", "evidence_bundle.json");
    if (!existsSync(bundleEvidenceAbs)) throw new Error(`Missing bundled evidence excerpt file: ${bundleEvidenceAbs}. Rebuild the bundle.`);
    const j = await readJsonAbs(bundleEvidenceAbs);
    const evIn = Array.isArray(j?.evidence) ? j.evidence : [];
    evidence = evIn
      .filter((e) => e && typeof e === "object" && typeof e.evidence_id === "string" && typeof e.excerpt === "string")
      .map((e) => ({
        evidence_id: String(e.evidence_id),
        file_path: String(e.file_path || ""),
        commit_sha: String(e.commit_sha || ""),
        start_line: e.start_line,
        end_line: e.end_line,
        excerpt: String(e.excerpt || ""),
      }))
      .filter((e) => evidenceIds.has(e.evidence_id))
      .sort((a, b) => a.evidence_id.localeCompare(b.evidence_id));
    if (!evidence.length) throw new Error("Bundled evidence excerpt list is empty or does not match evidence_refs.jsonl.");
  } else {
    const repoAbs = resolveRepoAbs({ reposJson, repoId });
    evidence = buildEvidenceBundle({ repoAbs, refs });
  }

  const kickoff = await loadKickoffInputs({ knowledgeRootAbs: readPaths.rootAbs, scope: `repo:${repoId}` });

  const profilesRes = await loadLlmProfiles();
  if (!profilesRes.ok) throw new Error(profilesRes.message);
  const profiles = profilesRes.profiles;

  const archProfile = resolveLlmProfileOrError({ profiles, profileKey: "committee.repo_architect" });
  if (!archProfile.ok) throw new Error(archProfile.message);
  const skepticProfile = resolveLlmProfileOrError({ profiles, profileKey: "committee.repo_skeptic" });
  if (!skepticProfile.ok) throw new Error(skepticProfile.message);

  const archClient = createLlmClient({ ...archProfile.profile, temperature: 0 });
  if (!archClient.ok) throw new Error(archClient.message);
  const skepticClient = createLlmClient({ ...skepticProfile.profile, temperature: 0 });
  if (!skepticClient.ok) throw new Error(skepticClient.message);

  const outDirAbs = join(paths.knowledge.ssotReposAbs, repoId, "committee");
  const archJsonAbs = join(outDirAbs, "architect_claims.json");
  const archMdAbs = join(outDirAbs, "architect_claims.md");
  const archErrAbs = join(outDirAbs, "architect_claims.error.json");
  const skJsonAbs = join(outDirAbs, "skeptic_challenges.json");
  const skMdAbs = join(outDirAbs, "skeptic_challenges.md");
  const skErrAbs = join(outDirAbs, "skeptic_challenges.error.json");
  const statusAbs = join(outDirAbs, "committee_status.json");

  const created_at = nowISO();

  const architectSystem = readCommitteePrompt("src/llm/prompts/committee/repo_architect.system.txt");

  const priorDecisions = await loadAnsweredDecisions({ decisionsDirAbs: readPaths.decisionsAbs, scopes: ["system", `repo:${repoId}`] });
  const architectUser = {
    repo_id: repoId,
    kickoff_system: kickoff.system && kickoff.system.inputs ? kickoff.system.inputs : null,
    kickoff_repo: kickoff.repo && kickoff.repo.inputs ? kickoff.repo.inputs : null,
    prior_decisions: priorDecisions,
    repo_index: repoIndex,
    allowed_evidence_ids: Array.from(evidenceIds).sort((a, b) => a.localeCompare(b)),
    evidence_bundle: evidence.map((e) => ({ evidence_id: e.evidence_id, file_path: e.file_path, start_line: e.start_line, end_line: e.end_line, excerpt: e.excerpt })),
  };

  const archMessages = await buildAugmentedCommitteeMessages({
    paths,
    scope: `repo:${repoId}`,
    systemPrompt: architectSystem,
    userPrompt: JSON.stringify(architectUser),
    context: { role: "committee.repo_architect", repo_id: repoId },
  });
  const archResp = await archClient.llm.invoke(archMessages);

  let architectOut = null;
  try {
    architectOut = parseAndValidateCommitteeOutput({
      rawText: archResp?.content,
      expectedScope: `repo:${repoId}`,
      allowedEvidenceIds: evidenceIds,
    });
    if (staleInfo.stale) {
      architectOut = capCommitteeOutput(applySoftStaleMarker(architectOut, staleInfo));
      validateCommitteeOutput(architectOut);
      validateEvidenceRefsMembership({ output: architectOut, allowedEvidenceIds: evidenceIds });
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const next_action = msg.includes("unknown evidence_ref") ? "rescan_needed" : "decision_needed";
    const severity = next_action === "rescan_needed" ? "medium" : "high";
    const status = {
      version: 1,
      repo_id: repoId,
      evidence_valid: false,
      blocking_issues: [
        {
          id: stableId("ISSUE", [repoId, "repo_architect output invalid", msg, next_action]),
          description: `repo_architect output invalid: ${msg}`,
          evidence_missing: next_action === "rescan_needed" ? ["need evidence: file: (committee referenced evidence not in allowed set; regenerate evidence_refs.jsonl and rerun committee)"] : ["need evidence: file: (fix committee prompt/output format; rerun committee)"],
          severity,
        },
      ],
      confidence: "low",
      next_action,
    };
    const statusWithStale = applySoftStaleStatusMeta(status, repoScopeStaleness);
    validateCommitteeStatus(statusWithStale);
    if (!dryRun) {
      await mkdir(outDirAbs, { recursive: true });
      await writeTextAtomic(archErrAbs, JSON.stringify({ ok: false, role: "repo_architect", captured_at: created_at, message: msg }, null, 2) + "\n");
      await writeTextAtomic(statusAbs, JSON.stringify(statusWithStale, null, 2) + "\n");
    }
    const decisionPackets =
      statusWithStale.next_action === "decision_needed"
        ? await writeDecisionPackets({
            decisionsDirAbs: paths.knowledge.decisionsAbs,
            repoId,
            blockers: statusWithStale.blocking_issues.filter((b) => b.severity === "high"),
            evidenceFallbackIds: Array.from(evidenceIds).slice(0, 1),
            dryRun,
          })
        : [];
    return {
      ok: false,
      repo_id: repoId,
      evidence_valid: false,
      next_action: statusWithStale.next_action,
      message: `repo_architect output invalid: ${msg}`,
      decisions_written: decisionPackets,
    };
  }

  const skepticSystem = readCommitteePrompt("src/llm/prompts/committee/repo_skeptic.system.txt");

  const skepticUser = {
    repo_id: repoId,
    architect_output: architectOut,
    prior_decisions: priorDecisions,
    allowed_evidence_ids: Array.from(evidenceIds).sort((a, b) => a.localeCompare(b)),
    evidence_bundle: evidence.map((e) => ({ evidence_id: e.evidence_id, file_path: e.file_path, excerpt: e.excerpt })),
    kickoff_system: kickoff.system && kickoff.system.inputs ? kickoff.system.inputs : null,
    kickoff_repo: kickoff.repo && kickoff.repo.inputs ? kickoff.repo.inputs : null,
  };

  const skepticMessages = await buildAugmentedCommitteeMessages({
    paths,
    scope: `repo:${repoId}`,
    systemPrompt: skepticSystem,
    userPrompt: JSON.stringify(skepticUser),
    context: { role: "committee.repo_skeptic", repo_id: repoId },
  });
  const skResp = await skepticClient.llm.invoke(skepticMessages);

  let skepticOut = null;
  try {
    skepticOut = parseAndValidateCommitteeOutput({
      rawText: skResp?.content,
      expectedScope: `repo:${repoId}`,
      allowedEvidenceIds: evidenceIds,
    });
    if (staleInfo.stale) {
      skepticOut = capCommitteeOutput(applySoftStaleMarker(skepticOut, staleInfo));
      validateCommitteeOutput(skepticOut);
      validateEvidenceRefsMembership({ output: skepticOut, allowedEvidenceIds: evidenceIds });
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const next_action = msg.includes("unknown evidence_ref") ? "rescan_needed" : "decision_needed";
    const severity = next_action === "rescan_needed" ? "medium" : "high";
    const status = {
      version: 1,
      repo_id: repoId,
      evidence_valid: false,
      blocking_issues: [
        {
          id: stableId("ISSUE", [repoId, "repo_skeptic output invalid", msg, next_action]),
          description: `repo_skeptic output invalid: ${msg}`,
          evidence_missing: next_action === "rescan_needed" ? ["need evidence: file: (committee referenced evidence not in allowed set; regenerate evidence_refs.jsonl and rerun committee)"] : ["need evidence: file: (fix committee prompt/output format; rerun committee)"],
          severity,
        },
      ],
      confidence: "low",
      next_action,
    };
    const statusWithStale = applySoftStaleStatusMeta(status, repoScopeStaleness);
    validateCommitteeStatus(statusWithStale);
    if (!dryRun) {
      await mkdir(outDirAbs, { recursive: true });
      await writeTextAtomic(skErrAbs, JSON.stringify({ ok: false, role: "repo_skeptic", captured_at: created_at, message: msg }, null, 2) + "\n");
      await writeTextAtomic(statusAbs, JSON.stringify(statusWithStale, null, 2) + "\n");
    }
    const decisionPackets =
      statusWithStale.next_action === "decision_needed"
        ? await writeDecisionPackets({
            decisionsDirAbs: paths.knowledge.decisionsAbs,
            repoId,
            blockers: statusWithStale.blocking_issues.filter((b) => b.severity === "high"),
            evidenceFallbackIds: Array.from(evidenceIds).slice(0, 1),
            dryRun,
          })
        : [];
    return {
      ok: false,
      repo_id: repoId,
      evidence_valid: false,
      next_action: statusWithStale.next_action,
      message: `repo_skeptic output invalid: ${msg}`,
      decisions_written: decisionPackets,
    };
  }

  const status = applySoftStaleStatusMeta(deriveCommitteeStatusFromOutputs({ repoId, architect: architectOut, skeptic: skepticOut }), repoScopeStaleness);
  validateCommitteeStatus(status);

  if (!dryRun) {
    const archMd = maybePrependSoftStaleBanner({
      markdown: renderClaimsMd({ repo_id: repoId, created_at, claims: architectOut, role: "repo_architect" }),
      stalenessSnapshot: repoScopeStaleness,
      repoSnapshot: staleInfo,
    });
    const skepticMd = maybePrependSoftStaleBanner({
      markdown: renderChallengesMd({ repo_id: repoId, created_at, challenges: skepticOut }),
      stalenessSnapshot: repoScopeStaleness,
      repoSnapshot: staleInfo,
    });
    await mkdir(outDirAbs, { recursive: true });
    await writeTextAtomic(archJsonAbs, JSON.stringify(architectOut, null, 2) + "\n");
    await writeTextAtomic(archMdAbs, archMd);
    await writeTextAtomic(skJsonAbs, JSON.stringify(skepticOut, null, 2) + "\n");
    await writeTextAtomic(skMdAbs, skepticMd);
    await writeTextAtomic(statusAbs, JSON.stringify(status, null, 2) + "\n");
    try {
      await rm(join(outDirAbs, "STALE.json"), { force: true });
    } catch {
      // ignore
    }
    try {
      await rm(archErrAbs, { force: true });
      await rm(skErrAbs, { force: true });
    } catch {
      // ignore
    }
  }

  const decisionPackets =
    status.next_action === "decision_needed"
      ? await writeDecisionPackets({
          decisionsDirAbs: paths.knowledge.decisionsAbs,
          repoId,
          blockers: status.blocking_issues.filter((b) => b.severity === "high"),
          evidenceFallbackIds: Array.from(evidenceIds).slice(0, 1),
          dryRun,
        })
      : [];

  return {
    ok: true,
    repo_id: repoId,
    evidence_valid: status.evidence_valid,
    next_action: status.next_action,
    stale: staleInfo.stale === true,
    hard_stale: staleInfo.hard_stale === true,
    out: {
      architect_claims_json: `ssot/repos/${repoId}/committee/architect_claims.json`,
      skeptic_challenges_json: `ssot/repos/${repoId}/committee/skeptic_challenges.json`,
      committee_status_json: `ssot/repos/${repoId}/committee/committee_status.json`,
    },
    decisions_written: decisionPackets,
  };
}

function deriveIntegrationStatusFromChairOutput({ chairOut, maxGaps = 15 }) {
  const gaps = [];
  for (const ed of Array.isArray(chairOut.integration_edges) ? chairOut.integration_edges : []) {
    const miss = normalizeEvidenceMissingList(ed.evidence_missing);
    if (!miss.length) continue;
    const from = normStr(ed.from);
    const to = normStr(ed.to);
    const fromRepo = from.startsWith("repo:") ? from.slice("repo:".length) : null;
    const toRepo = to.startsWith("repo:") ? to.slice("repo:".length) : null;
    if (!fromRepo || !toRepo) continue;
    const repos = Array.from(new Set([fromRepo, toRepo])).sort((a, b) => a.localeCompare(b));
    const refs = normalizeEvidenceRefsList(ed.evidence_refs);
    const confidence = typeof ed.confidence === "number" && Number.isFinite(ed.confidence) ? ed.confidence : 0;
    const severity = confidence < 0.35 ? "high" : confidence < 0.6 ? "medium" : "low";
    const description = `Integration edge needs evidence (${normStr(ed.type)}): ${normStr(ed.contract)}`;
    gaps.push({
      id: stableId("GAP", [repos.join(","), description, severity, refs.join(","), miss.join(",")]),
      repos,
      description,
      evidence_refs: refs,
      severity,
    });
  }

  gaps.sort((a, b) => a.id.localeCompare(b.id));
  const capped = gaps.slice(0, maxGaps);

  const hasHigh = capped.some((g) => g.severity === "high");
  const decision_needed =
    chairOut.verdict !== "evidence_valid" ||
    capped.length > 0 ||
    (Array.isArray(chairOut.assumptions) && chairOut.assumptions.length > 0) ||
    (Array.isArray(chairOut.unknowns) && chairOut.unknowns.length > 0);

  const evidence_valid = chairOut.verdict === "evidence_valid" && !hasHigh;
  const status = { version: 1, evidence_valid, integration_gaps: capped, decision_needed };
  validateIntegrationStatus(status);
  return status;
}

async function runIntegrationChair({ paths, activeRepoIds, dryRun, bundleId = null, forceStaleOverride = false } = {}) {
  const sysBundleId = normStr(bundleId) || readLatestBundleIdForScope({ laneARootAbs: paths.laneA.rootAbs, scope: "system" });
  const sysRead = sysBundleId ? knowledgeReadSystemPathsFromBundle({ laneARootAbs: paths.laneA.rootAbs, bundleId: sysBundleId }) : null;

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) throw new Error(reposRes.message);
  const reposJson = reposRes.registry;
  const staleRepoInfos = [];
  for (const repoId of activeRepoIds) {
    // eslint-disable-next-line no-await-in-loop
    const s = await evaluateRepoStaleness({ paths, registry: reposJson, repoId });
    staleRepoInfos.push(s);
  }
  const systemScopeStaleness = buildSystemSoftStaleSnapshotFromRepoInfos(staleRepoInfos);
  await recordSoftStaleObservation({
    paths,
    scope: "system",
    stalenessSnapshot: systemScopeStaleness,
  });
  const systemBannerRepoSnapshot = selectSoftStaleBannerRepoSnapshot(staleRepoInfos);

  const integrationMapMissing = !sysRead || !existsSync(sysRead.integrationMapAbs);
  if (integrationMapMissing) {
    const created_at = nowISO();
    const outDirAbs = join(paths.knowledge.ssotSystemAbs, "committee", "integration");
    const findingsAbs = join(outDirAbs, "integration_findings.json");
    const mdAbs = join(outDirAbs, "integration_findings.md");
    const statusAbs = join(outDirAbs, "integration_status.json");

    const chairOut = {
      scope: "system",
      facts: [],
      assumptions: [],
      unknowns: [
        {
          text: "Integration chair requires an explicit integration map bundle to proceed.",
          evidence_missing: ["need integration_map.json bundle (run --knowledge-index then --knowledge-bundle --scope system)"],
        },
      ],
      integration_edges: [],
      risks: [],
      verdict: "evidence_invalid",
    };
    validateCommitteeOutput(chairOut);
    const status = applySoftStaleStatusMeta(deriveIntegrationStatusFromChairOutput({ chairOut, maxGaps: 15 }), systemScopeStaleness);
    validateIntegrationStatus(status);
    if (!dryRun) {
      const findingsMd = maybePrependSoftStaleBanner({
        markdown: renderIntegrationMd({ created_at, gaps: chairOut }),
        stalenessSnapshot: systemScopeStaleness,
        repoSnapshot: systemBannerRepoSnapshot,
      });
      await mkdir(outDirAbs, { recursive: true });
      await writeTextAtomic(findingsAbs, JSON.stringify(chairOut, null, 2) + "\n");
      await writeTextAtomic(mdAbs, findingsMd);
      await writeTextAtomic(statusAbs, JSON.stringify(status, null, 2) + "\n");
    }
    return { ok: false, evidence_valid: false, decision_needed: true, gaps_count: status.integration_gaps.length, decisions_written: [] };
  }

  const hardStale = staleRepoInfos.filter((s) => s && s.hard_stale === true);
  if (hardStale.length && !forceStaleOverride) {
    const decisions_written = [];
    for (const s of hardStale) {
      // eslint-disable-next-line no-await-in-loop
      const decision = await writeRefreshRequiredDecisionPacketIfNeeded({
        paths,
        repoId: s.repo_id,
        blockingState: "COMMITTEE_PENDING",
        staleInfo: s,
        producer: "committee",
        dryRun,
      });
      if (decision?.json_abs) decisions_written.push(decision.json_abs);
    }
    return { ok: false, evidence_valid: false, decision_needed: true, gaps_count: 0, reason_code: "STALE_BLOCKED", decisions_written };
  }
  const softStale = staleRepoInfos.filter((s) => s && s.stale === true);

  const profilesRes = await loadLlmProfiles();
  if (!profilesRes.ok) throw new Error(profilesRes.message);
  const profiles = profilesRes.profiles;
  const chairProfile = resolveLlmProfileOrError({ profiles, profileKey: "committee.integration_chair" });
  if (!chairProfile.ok) throw new Error(chairProfile.message);
  const chairClient = createLlmClient({ ...chairProfile.profile, temperature: 0 });
  if (!chairClient.ok) throw new Error(chairClient.message);

  // Build allowed evidence IDs from repo committees (architect outputs).
  const allowedEvidenceIds = new Set();
  const repoSummaries = [];
  for (const repoId of activeRepoIds) {
    const dir = join(paths.knowledge.ssotReposAbs, repoId, "committee");
    const archAbs = join(dir, "architect_claims.json");
    const skAbs = join(dir, "skeptic_challenges.json");
    if (!existsSync(archAbs) || !existsSync(skAbs)) throw new Error(`Missing repo committee outputs for ${repoId}.`);
    // eslint-disable-next-line no-await-in-loop
    const arch = await readJsonAbs(archAbs);
    // eslint-disable-next-line no-await-in-loop
    const sk = await readJsonAbs(skAbs);
    validateCommitteeOutput(arch);
    validateCommitteeOutput(sk);
    for (const e of collectEvidenceRefsFromOutput(arch)) allowedEvidenceIds.add(String(e));
    for (const e of collectEvidenceRefsFromOutput(sk)) allowedEvidenceIds.add(String(e));
    repoSummaries.push({ repo_id: repoId, architect_output: arch, skeptic_output: sk });
  }

  const kickoff = await loadKickoffInputs({ knowledgeRootAbs: paths.knowledge.rootAbs, scope: "system" });
  const sysKickoff = kickoff.system && kickoff.system.inputs ? kickoff.system.inputs : null;

  const systemPrompt = readCommitteePrompt("src/llm/prompts/committee/integration_chair.system.txt");

  const integrationMap = JSON.parse(String(readFileSync(sysRead.integrationMapAbs, "utf8") || ""));

  const user = {
    kickoff_system: sysKickoff,
    integration_map: integrationMap,
    allowed_evidence_ids: Array.from(allowedEvidenceIds).sort((a, b) => a.localeCompare(b)),
    prior_decisions: await loadAnsweredDecisions({ decisionsDirAbs: paths.knowledge.decisionsAbs, scopes: ["system"] }),
    repo_committees: repoSummaries,
  };

  const chairMessages = await buildAugmentedCommitteeMessages({
    paths,
    scope: "system",
    systemPrompt,
    userPrompt: JSON.stringify(user),
    context: { role: "committee.integration_chair", scope: "system" },
  });
  const resp = await chairClient.llm.invoke(chairMessages);

  const created_at = nowISO();
  const outDirAbs = join(paths.knowledge.ssotSystemAbs, "committee", "integration");
  const findingsAbs = join(outDirAbs, "integration_findings.json");
  const mdAbs = join(outDirAbs, "integration_findings.md");
  const statusAbs = join(outDirAbs, "integration_status.json");
  const errAbs = join(outDirAbs, "integration_findings.error.json");

  let chairOut = null;
  try {
    chairOut = parseAndValidateCommitteeOutput({
      rawText: resp?.content,
      expectedScope: "system",
      allowedEvidenceIds,
    });
    if (softStale.length) {
      chairOut = capCommitteeOutput(applySoftStaleMarkerSystem(chairOut, softStale));
      validateCommitteeOutput(chairOut);
      validateEvidenceRefsMembership({ output: chairOut, allowedEvidenceIds });
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const status = applySoftStaleStatusMeta({ version: 1, evidence_valid: false, integration_gaps: [], decision_needed: true }, systemScopeStaleness);
    validateIntegrationStatus(status);
    if (!dryRun) {
      await mkdir(outDirAbs, { recursive: true });
      await writeTextAtomic(errAbs, JSON.stringify({ ok: false, role: "integration_chair", captured_at: created_at, message: msg }, null, 2) + "\n");
      await writeTextAtomic(statusAbs, JSON.stringify(status, null, 2) + "\n");
    }
    return { ok: false, evidence_valid: false, decision_needed: true, gaps_count: 0, message: `integration_chair output invalid: ${msg}` };
  }

  const status = applySoftStaleStatusMeta(deriveIntegrationStatusFromChairOutput({ chairOut, maxGaps: 15 }), systemScopeStaleness);
  validateIntegrationStatus(status);
  const gaps = status.integration_gaps;

  if (!dryRun) {
    const findingsMd = maybePrependSoftStaleBanner({
      markdown: renderIntegrationMd({ created_at, gaps: chairOut }),
      stalenessSnapshot: systemScopeStaleness,
      repoSnapshot: systemBannerRepoSnapshot,
    });
    await mkdir(outDirAbs, { recursive: true });
    await writeTextAtomic(findingsAbs, JSON.stringify(chairOut, null, 2) + "\n");
    await writeTextAtomic(mdAbs, findingsMd);
    await writeTextAtomic(statusAbs, JSON.stringify(status, null, 2) + "\n");
    try {
      await rm(errAbs, { force: true });
    } catch {
      // ignore
    }
  }

  const decisionPackets =
    status.decision_needed === true
      ? await writeDecisionPackets({
          decisionsDirAbs: paths.knowledge.decisionsAbs,
          repoId: null,
          blockers:
            gaps.filter((g) => g.severity === "high").length > 0
              ? gaps.filter((g) => g.severity === "high")
              : [
                  {
                    id: stableId("ISSUE", ["system", "integration_decision_needed"]),
                    description: "Integration chair indicates a blocking decision is needed, but no specific high-severity gap was emitted.",
                    severity: "high",
                    repos: [],
                    evidence_refs: [],
                  },
                ],
          evidenceFallbackIds: Array.from(allowedEvidenceIds).slice(0, 1),
          dryRun,
        })
      : [];

  return { ok: status.evidence_valid, evidence_valid: status.evidence_valid, decision_needed: status.decision_needed, gaps_count: gaps.length, decisions_written: decisionPackets };
}

async function readCommitteeStatusOptional(absPath) {
  if (!existsSync(absPath)) return { ok: true, exists: false, status: null };
  try {
    const raw = await readFile(absPath, "utf8");
    if (!raw.trim()) return { ok: true, exists: false, status: null, was_invalid: true, error: "empty_file" };
    const j = JSON.parse(raw);
    validateCommitteeStatus(j);
    return { ok: true, exists: true, status: j };
  } catch (err) {
    return {
      ok: true,
      exists: false,
      status: null,
      was_invalid: true,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

async function readIntegrationStatusOptional(absPath) {
  if (!existsSync(absPath)) return { ok: true, exists: false, status: null };
  try {
    const raw = await readFile(absPath, "utf8");
    if (!raw.trim()) return { ok: true, exists: false, status: null, was_invalid: true, error: "empty_file" };
    const j = JSON.parse(raw);
    validateIntegrationStatus(j);
    return { ok: true, exists: true, status: j };
  } catch (err) {
    return {
      ok: true,
      exists: false,
      status: null,
      was_invalid: true,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

async function runQaStrategistCommittee({ paths, reposJson, parsedScope, bundleId = null, dryRun = false } = {}) {
  const expectedScope = parsedScope.scope;

  const profilesRes = await loadLlmProfiles();
  if (!profilesRes.ok) throw new Error(profilesRes.message);
  const profiles = profilesRes.profiles;
  const prof = resolveLlmProfileOrError({ profiles, profileKey: "committee.qa_strategist" });
  if (!prof.ok) throw new Error(prof.message);
  const client = createLlmClient({ ...prof.profile, temperature: 0 });
  if (!client.ok) throw new Error(client.message);

  const systemPrompt = readQaStrategistPrompt();

  const bid = typeof bundleId === "string" && bundleId.trim() ? bundleId.trim() : null;
  const readPaths =
    parsedScope.kind === "repo" && bid
      ? knowledgeReadPathsFromBundle({ laneARootAbs: paths.laneA.rootAbs, parsedScope, bundleId: bid })
      : bid && parsedScope.kind === "system"
        ? knowledgeReadSystemPathsFromBundle({ laneARootAbs: paths.laneA.rootAbs, bundleId: bid })
        : paths.knowledge;

  const payload = {
    role: "qa_strategist",
    scope: expectedScope,
    created_at: nowISO(),
    knowledge_root: readPaths.rootAbs,
  };

  if (parsedScope.kind === "repo") {
    const repoId = parsedScope.repo_id;
    const idxAbs = join(readPaths.evidenceIndexReposAbs, repoId, "repo_index.json");
    if (!existsSync(idxAbs)) throw new Error(`Missing repo_index.json for ${repoId} at ${idxAbs}. Run --knowledge-index.`);
    const repoIndex = JSON.parse(String(readFileSync(idxAbs, "utf8") || ""));
    validateRepoIndex(repoIndex);
    payload.repo_id = repoId;
    payload.repo_index = repoIndex;

    const scanAbs = join(readPaths.ssotReposAbs, repoId, "scan.json");
    if (existsSync(scanAbs)) {
      try {
        payload.repo_scan = await readJsonAbs(scanAbs);
      } catch {
        payload.repo_scan = null;
      }
    }

    const stAbs = join(readPaths.ssotReposAbs, repoId, "committee", "committee_status.json");
    if (existsSync(stAbs)) {
      try {
        payload.repo_committee_status = await readJsonAbs(stAbs);
      } catch {
        payload.repo_committee_status = null;
      }
    }
  } else {
    const integMapAbs = readPaths.integrationMapAbs || join(readPaths.rootAbs, "views", "integration_map.json");
    if (integMapAbs && existsSync(integMapAbs)) {
      try {
        payload.integration_map = JSON.parse(String(readFileSync(integMapAbs, "utf8") || ""));
      } catch {
        payload.integration_map = null;
      }
    }
    payload.active_repo_ids = listActiveRepoIds(reposJson);
  }

  const strategistMessages = await buildAugmentedCommitteeMessages({
    paths,
    scope: expectedScope,
    systemPrompt,
    userPrompt: JSON.stringify(payload),
    context: { role: "committee.qa_strategist", scope: expectedScope },
  });
  const resp = await client.llm.invoke(strategistMessages);

  const out = parseAndValidateQaCommitteeOutput({ rawText: resp?.content, expectedScope });

  const ts = nowFsSafeUtcTimestamp();
  const outDirAbs = join(paths.laneA.rootAbs, "committee", ts);
  const scopeKey = scopeToDirName(parsedScope);
  const jsonAbs = join(outDirAbs, `qa_strategist.${scopeKey}.json`);
  const mdAbs = join(outDirAbs, `qa_strategist.${scopeKey}.md`);
  if (!dryRun) {
    await mkdir(outDirAbs, { recursive: true });
    await writeTextAtomic(jsonAbs, JSON.stringify(out, null, 2) + "\n");
    await writeTextAtomic(mdAbs, renderQaStrategistMd(out) + "\n");
  }

  return { ok: true, role: "qa_strategist", scope: expectedScope, json: jsonAbs, md: mdAbs, dry_run: dryRun };
}

export async function runKnowledgeCommittee({
  projectRoot,
  scope = "system",
  bundleId = null,
  limit = null,
  mode = "run",
  maxQuestions = null,
  dryRun = false,
  forceStaleOverride = false,
  by = null,
  reason = null,
} = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  // Lane A consumers must read (but never mutate) SUFFICIENCY.json for gating and UI consistency.
  await readSufficiencyOrDefault({ projectRoot: paths.opsRootAbs });
  const parsedScope = validateScope(scope);
  const phase = await resolveLaneAPhaseForCommittee({ paths });

  if (phase === "none") {
    return {
      ok: true,
      dry_run: dryRun,
      scope: parsedScope.scope,
      phase,
      executed: [],
      message: "Reverse phase is closed and forward phase is not started. Run --knowledge-kickoff-forward to start forward work.",
      knowledge_root: paths.knowledge.rootAbs,
    };
  }

  const m = normStr(mode) || "run";
  if (m === "qa_strategist" || m === "qa-strategist" || m === "qa") {
    const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
    if (!reposRes.ok) return { ok: false, message: reposRes.message };
    const res = await runQaStrategistCommittee({ paths, reposJson: reposRes.registry, parsedScope, bundleId, dryRun });
    return { ok: res.ok, dry_run: dryRun, scope: parsedScope.scope, phase, mode: "qa_strategist", executed: [res], knowledge_root: paths.knowledge.rootAbs };
  }
  if (m === "challenge") {
    const q = await computeChallengeQuestion({ paths });
    const scopeDir = scopeToDirName(parsedScope);
    const ts = nowFsSafeUtcTimestamp();
    const questionsDirAbs = join(paths.laneA.rootAbs, "committee", scopeDir, "questions");
    const qId = stableId([parsedScope.scope, q.stage, q.question].join("\n")).slice(0, 16);
    const mdAbs = join(questionsDirAbs, `Q-${ts}.md`);
    const md = [
      `# Committee challenge question`,
      ``,
      `scope: ${parsedScope.scope}`,
      `stage: ${q.stage}`,
      `id: Q_${qId}`,
      `created_at: ${nowISO()}`,
      ``,
      `## Question`,
      ``,
      q.question,
      ``,
      `## Why now`,
      ``,
      q.why,
      ``,
    ].join("\n");
    const mx = maxQuestions == null ? 1 : clampInt(maxQuestions, { min: 1, max: 25 });
    if (!dryRun) {
      await mkdir(questionsDirAbs, { recursive: true });
      await writeTextAtomic(mdAbs, md + "\n");
    }
    return { ok: true, dry_run: dryRun, scope: parsedScope.scope, phase, mode: "challenge", asked: 1, max_questions: mx, question: { id: `Q_${qId}`, stage: q.stage, question: q.question, why_now: q.why, file: mdAbs }, knowledge_root: paths.knowledge.rootAbs };
  }

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, message: reposRes.message };
  const reposJson = reposRes.registry;
  const activeRepoIds = listActiveRepoIds(reposJson);
  const scopeRepoIds = parsedScope.kind === "repo" ? [parsedScope.repo_id] : activeRepoIds;
  const repoIds = scopeRepoIds.filter((id) => activeRepoIds.includes(id)).sort((a, b) => a.localeCompare(b));
  if (!repoIds.length) return { ok: false, message: "No repos in scope." };

  // If a stale override is used, record it once per invocation for auditability.
  if (forceStaleOverride) {
    const scopeKey = parsedScope.kind === "repo" ? `repo:${parsedScope.repo_id}` : "system";
    const st = await evaluateScopeStaleness({ paths, registry: reposJson, scope: scopeKey });
    if (st.stale) {
      const who = normStr(by) || normStr(process.env.USER || "") || null;
      const why = normStr(reason) || null;
      await appendFile("ai/lane_a/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), type: "stale_override", command: "knowledge-committee", scope: parsedScope.scope, by: who, reason: why }) + "\n");
    }
  }

  // When limit is not provided, process all missing repos in one run (single action: "repo_committee").
  // When limit is provided, it bounds the number of repo committees executed this run.
  const max = limit == null ? repoIds.length : clampInt(limit, { min: 0, max: repoIds.length });

  // Determine what to do next: process missing repo committees OR run integration chair.
  const missing = [];
  const failed = [];
  for (const repoId of repoIds) {
    const statusAbs = join(paths.knowledge.ssotReposAbs, repoId, "committee", "committee_status.json");
    // eslint-disable-next-line no-await-in-loop
    const st = await readCommitteeStatusOptional(statusAbs);
    if (!st.ok) return { ok: false, message: `Invalid committee_status.json for ${repoId}.` };
    if (!st.exists) missing.push(repoId);
    else if (st.status && st.status.evidence_valid === false) failed.push(repoId);
  }

  const concurrency = Math.max(1, Math.min(8, (cpus() || []).length || 4));
  const executed = [];

  // If committee output is missing OR previously failed (evidence_invalid), re-run the repo committee for those repos.
  // This keeps the runner idempotent but allows recovery after rescans/decision answers/prompt fixes.
  const pendingRepoCommittees = missing.concat(failed).sort((a, b) => a.localeCompare(b));

  if (pendingRepoCommittees.length) {
    const targets = pendingRepoCommittees.slice(0, max);
    const bid = typeof bundleId === "string" && bundleId.trim() ? bundleId.trim() : null;
    // bundle-id is only meaningful for repo committee runs. System committee runs may pass it for the later
    // integration chair step; do not block repo committee execution on it.
    if (bid && parsedScope.kind === "repo" && !(targets.length === 1 && targets[0] === parsedScope.repo_id)) {
      return { ok: false, dry_run: dryRun, scope: parsedScope.scope, executed, message: "--bundle-id is only supported for repo-scoped committee runs with one target.", knowledge_root: paths.knowledge.rootAbs };
    }
    const bidForRepo = parsedScope.kind === "repo" ? bid : null;
    const knowledgeRead = bidForRepo ? knowledgeReadPathsFromBundle({ laneARootAbs: paths.laneA.rootAbs, parsedScope, bundleId: bidForRepo }) : null;
    const results = await runWorkerPool({
      items: targets,
      concurrency: Math.min(concurrency, targets.length || 1),
      worker: async (repoId) => {
        try {
          const r = await runRepoCommittee({ paths, reposJson, repoId, dryRun, bundleId: bidForRepo, knowledgeRead, forceStaleOverride });
          return r;
        } catch (err) {
          return { ok: false, repo_id: repoId, message: err instanceof Error ? err.message : String(err) };
        }
      },
    });
    executed.push({ type: "repo_committee", targets, results });
    const okAll = results.every((r) => r && r.ok === true && r.evidence_valid === true && r.stale !== true);
    return { ok: okAll, dry_run: dryRun, scope: parsedScope.scope, phase, executed, concurrency, knowledge_root: paths.knowledge.rootAbs };
  }

  // All repos have passed committee_status. If system scope, run integration if missing.
  if (parsedScope.kind === "system") {
    const integAbs = join(paths.knowledge.ssotSystemAbs, "committee", "integration", "integration_status.json");
    const integ = await readIntegrationStatusOptional(integAbs);
    if (!integ.ok) return { ok: false, message: "Invalid integration_status.json." };
    if (!integ.exists || (integ.status && integ.status.evidence_valid === false)) {
      const sysBid = typeof bundleId === "string" && bundleId.trim() ? bundleId.trim() : null;
      const res = await runIntegrationChair({ paths, activeRepoIds: repoIds, dryRun, bundleId: sysBid, forceStaleOverride });
      executed.push({ type: "integration_chair", ok: res.ok, gaps_count: res.gaps_count, decision_needed: res.decision_needed });
      return { ok: res.ok, dry_run: dryRun, scope: parsedScope.scope, phase, executed, knowledge_root: paths.knowledge.rootAbs };
    }
  }

  return { ok: true, dry_run: dryRun, scope: parsedScope.scope, phase, executed, message: "No committee work needed.", knowledge_root: paths.knowledge.rootAbs };
}

export async function runKnowledgeCommitteeStatus({ projectRoot } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, message: reposRes.message };
  const reposJson = reposRes.registry;
  const activeRepoIds = listActiveRepoIds(reposJson);

  const repos = [];
  for (const repoId of activeRepoIds) {
    const statusAbs = join(paths.knowledge.ssotReposAbs, repoId, "committee", "committee_status.json");
    // eslint-disable-next-line no-await-in-loop
    const st = await readCommitteeStatusOptional(statusAbs);
    if (!st.ok) return { ok: false, message: `Invalid committee_status.json for ${repoId}.` };
    repos.push({
      repo_id: repoId,
      exists: st.exists,
      evidence_valid: st.exists ? st.status.evidence_valid : null,
      confidence: st.exists ? st.status.confidence : null,
      next_action: st.exists ? st.status.next_action : null,
      blocking_issues: st.exists ? st.status.blocking_issues.length : 0,
    });
  }

  const integAbs = join(paths.knowledge.ssotSystemAbs, "committee", "integration", "integration_status.json");
  const integ = await readIntegrationStatusOptional(integAbs);
  if (!integ.ok) return { ok: false, message: "Invalid integration_status.json." };

  const decisionsDir = paths.knowledge.decisionsAbs;
  let decisions = [];
  try {
    if (existsSync(decisionsDir)) {
      const entries = await readdir(decisionsDir, { withFileTypes: true });
      decisions = entries.filter((e) => e.isFile() && e.name.endsWith(".json")).map((e) => e.name).sort((a, b) => a.localeCompare(b));
    }
  } catch {
    decisions = [];
  }

  return {
    ok: true,
    knowledge_root: paths.knowledge.rootAbs,
    repos,
    integration: integ.exists ? integ.status : null,
    decisions: decisions.map((f) => `decisions/${f}`),
  };
}
