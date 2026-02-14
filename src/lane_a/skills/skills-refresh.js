import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { loadProjectPaths } from "../../paths/project-paths.js";
import { validateSkillPackage, validateSkillsRegistry } from "../../contracts/validators/index.js";
import { jsonStableStringify } from "../../utils/json.js";
import { nowFsSafeUtcTimestamp } from "../../utils/naming.js";

function nowIso() {
  return new Date().toISOString();
}

function normStr(value) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeLf(text) {
  return String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

function defaultAiTeamRepoRoot() {
  const envRoot = normStr(process.env.AI_TEAM_REPO);
  if (envRoot) return resolve(envRoot);
  return resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
}

function resolveAiTeamRepoRoot(aiTeamRepoRoot = null) {
  const root = normStr(aiTeamRepoRoot) || defaultAiTeamRepoRoot();
  if (!isAbsolute(root)) throw new Error(`aiTeamRepoRoot must be absolute (got: ${root}).`);
  return resolve(root);
}

async function readJsonAbs(absPath, nameForError) {
  let raw;
  try {
    raw = await readFile(absPath, "utf8");
  } catch (err) {
    if (err && err.code === "ENOENT") throw new Error(`${nameForError} missing at ${absPath}.`);
    throw err;
  }
  try {
    return JSON.parse(String(raw || ""));
  } catch {
    throw new Error(`${nameForError} is invalid JSON at ${absPath}.`);
  }
}

async function listKnownEvidenceIds(paths) {
  const reposRoot = paths.knowledge.evidenceReposAbs;
  const out = new Set();
  const entries = await readdir(reposRoot, { withFileTypes: true }).catch(() => []);
  for (const entry of entries.filter((item) => item.isDirectory()).sort((a, b) => a.name.localeCompare(b.name))) {
    const refsAbs = join(reposRoot, entry.name, "evidence_refs.jsonl");
    // eslint-disable-next-line no-await-in-loop
    const text = await readFile(refsAbs, "utf8").catch(() => "");
    for (const line of String(text).split("\n").map((part) => part.trim()).filter(Boolean)) {
      try {
        const parsed = JSON.parse(line);
        const id = normStr(parsed?.evidence_id);
        if (id) out.add(id);
      } catch {
        continue;
      }
    }
  }
  return out;
}

async function evaluateScopeStaleness({ paths, sourceScope }) {
  const scope = normStr(sourceScope);
  if (scope === "system") {
    const abs = join(paths.knowledge.ssotSystemAbs, "committee", "integration", "integration_status.json");
    const status = await readJsonAbs(abs, "integration_status.json").catch(() => null);
    if (!status || typeof status !== "object") return { stale: false, reasons: [] };
    const stale = status.stale === true || status.hard_stale === true || status.degraded === true;
    const reasons = Array.isArray(status.staleness?.reason_codes) ? status.staleness.reason_codes.map((value) => String(value)) : [];
    return { stale, reasons };
  }

  const match = scope.match(/^repo:([a-z0-9._-]+)$/i);
  if (!match) return { stale: false, reasons: [] };
  const repoId = match[1];
  const abs = join(paths.knowledge.ssotReposAbs, repoId, "committee", "committee_status.json");
  const status = await readJsonAbs(abs, "committee_status.json").catch(() => null);
  if (!status || typeof status !== "object") return { stale: false, reasons: [] };
  const stale = status.stale === true || status.hard_stale === true || status.degraded === true;
  const reasons = Array.isArray(status.staleness?.reason_codes) ? status.staleness.reason_codes.map((value) => String(value)) : [];
  return { stale, reasons };
}

function buildRefreshDraft({ skillId, sourceScope, reason, evidenceRefs, ts, sequence }) {
  const suffix = String(sequence).padStart(3, "0");
  const draftId = `DRAFT_REFRESH_${ts}_${suffix}`;
  return {
    version: 1,
    draft_id: draftId,
    scope: sourceScope,
    candidate_skill_id: skillId,
    reason,
    evidence_refs: Array.isArray(evidenceRefs) ? evidenceRefs : [],
    refresh_of: skillId,
    status: "pending",
    created_at: nowIso(),
    updated_at: nowIso(),
  };
}

export async function runSkillsRefresh({
  projectRoot,
  aiTeamRepoRoot = null,
  dryRun = false,
} = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  const aiTeamRepoRootAbs = resolveAiTeamRepoRoot(aiTeamRepoRoot);
  const registryAbs = join(aiTeamRepoRootAbs, "skills", "SKILLS.json");
  const registry = await readJsonAbs(registryAbs, "SKILLS.json");
  validateSkillsRegistry(registry);

  const draftsDirAbs = join(paths.laneA.skillsDirAbs, "drafts");
  const knownEvidenceIds = await listKnownEvidenceIds(paths);

  const skillIds = Object.keys(registry.skills || {}).sort((a, b) => a.localeCompare(b));
  let valid = 0;
  let stale = 0;
  let refreshDraftsCreated = 0;
  const refreshDrafts = [];
  const ts = nowFsSafeUtcTimestamp();
  let sequence = 0;

  for (const skillId of skillIds) {
    const skillMdAbs = join(aiTeamRepoRootAbs, "skills", skillId, "skill.md");
    const skillJsonAbs = join(aiTeamRepoRootAbs, "skills", skillId, "skill.json");

    let metadata = null;
    let skillMd = "";
    let hasValidationError = false;
    const staleReasons = [];

    try {
      // eslint-disable-next-line no-await-in-loop
      metadata = await readJsonAbs(skillJsonAbs, "skill.json");
      // eslint-disable-next-line no-await-in-loop
      skillMd = normalizeLf(await readFile(skillMdAbs, "utf8"));
      validateSkillPackage(metadata, { skillMd });
    } catch (err) {
      hasValidationError = true;
      staleReasons.push(`validation_failed:${err instanceof Error ? err.message : String(err)}`);
    }

    const evidenceRefs = Array.isArray(metadata?.evidence_refs) ? metadata.evidence_refs.map((value) => String(value).trim()).filter(Boolean) : [];
    const missingEvidenceRefs = evidenceRefs.filter((id) => !knownEvidenceIds.has(id));
    if (missingEvidenceRefs.length) staleReasons.push(`missing_evidence:${missingEvidenceRefs.join(",")}`);

    // eslint-disable-next-line no-await-in-loop
    const staleInfo = await evaluateScopeStaleness({ paths, sourceScope: metadata?.source_scope || registry.skills[skillId]?.source_scope || "system" });
    if (staleInfo.stale) staleReasons.push(`knowledge_stale:${staleInfo.reasons.join(",") || "true"}`);

    const isStale = hasValidationError || missingEvidenceRefs.length > 0 || staleInfo.stale;
    if (!isStale) {
      valid += 1;
      continue;
    }

    stale += 1;
    sequence += 1;
    const sourceScope = normStr(metadata?.source_scope) || "system";
    const reason = staleReasons.join(" | ").slice(0, 1500);
    const refreshDraft = buildRefreshDraft({
      skillId,
      sourceScope,
      reason,
      evidenceRefs,
      ts,
      sequence,
    });
    const outAbs = join(draftsDirAbs, `${refreshDraft.draft_id}.json`);
    refreshDrafts.push(outAbs);
    if (!dryRun) {
      // eslint-disable-next-line no-await-in-loop
      await mkdir(draftsDirAbs, { recursive: true });
      // eslint-disable-next-line no-await-in-loop
      await writeFile(outAbs, jsonStableStringify(refreshDraft), "utf8");
    }
    refreshDraftsCreated += 1;
  }

  return {
    ok: true,
    dry_run: !!dryRun,
    checked: skillIds.length,
    valid,
    stale,
    refresh_drafts_created: refreshDraftsCreated,
    refresh_drafts: refreshDrafts,
  };
}

