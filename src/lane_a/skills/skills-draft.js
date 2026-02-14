import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { loadProjectPaths } from "../../paths/project-paths.js";
import { nowFsSafeUtcTimestamp } from "../../utils/naming.js";
import { jsonStableStringify } from "../../utils/json.js";
import { validateScope } from "../knowledge/knowledge-utils.js";

function nowIso() {
  return new Date().toISOString();
}

function normStr(value) {
  return typeof value === "string" ? value.trim() : "";
}

function slugifyToken(value) {
  return normStr(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

async function readJsonAbs(absPath) {
  const raw = await readFile(absPath, "utf8");
  return JSON.parse(String(raw || ""));
}

async function readEvidenceIdsForRepo(paths, repoId) {
  const abs = join(paths.knowledge.evidenceReposAbs, repoId, "evidence_refs.jsonl");
  const text = await readFile(abs, "utf8").catch(() => "");
  const ids = [];
  for (const line of String(text).split("\n").map((part) => part.trim()).filter(Boolean)) {
    try {
      const parsed = JSON.parse(line);
      const evidenceId = normStr(parsed?.evidence_id);
      if (evidenceId) ids.push(evidenceId);
    } catch {
      continue;
    }
  }
  return Array.from(new Set(ids)).sort((a, b) => a.localeCompare(b));
}

async function readEvidenceIdsForSystem(paths) {
  const reposDir = paths.knowledge.evidenceReposAbs;
  const entries = await readdir(reposDir, { withFileTypes: true }).catch(() => []);
  const out = [];
  for (const entry of entries.filter((item) => item.isDirectory()).sort((a, b) => a.name.localeCompare(b.name))) {
    // eslint-disable-next-line no-await-in-loop
    const ids = await readEvidenceIdsForRepo(paths, entry.name);
    out.push(...ids.slice(0, 8));
  }
  return Array.from(new Set(out)).sort((a, b) => a.localeCompare(b)).slice(0, 32);
}

function draftShape({ draftId, scope, candidateSkillId, reason, evidenceRefs }) {
  return {
    version: 1,
    draft_id: draftId,
    scope,
    candidate_skill_id: candidateSkillId,
    reason,
    evidence_refs: evidenceRefs,
    status: "pending",
    created_at: nowIso(),
    updated_at: nowIso(),
  };
}

async function deriveRepoDraft({ paths, repoId, scope }) {
  const evidenceRefs = await readEvidenceIdsForRepo(paths, repoId);
  if (!evidenceRefs.length) throw new Error(`No evidence refs available for ${scope}. Run --knowledge-scan first.`);

  const repoIndexAbs = join(paths.knowledge.evidenceIndexReposAbs, repoId, "repo_index.json");
  const repoIndex = await readJsonAbs(repoIndexAbs).catch(() => null);
  const openapiCount = Array.isArray(repoIndex?.api_surface?.openapi_files) ? repoIndex.api_surface.openapi_files.length : 0;
  const routeCount = Array.isArray(repoIndex?.api_surface?.routes_controllers) ? repoIndex.api_surface.routes_controllers.length : 0;
  const migrationCount = Array.isArray(repoIndex?.migrations_schema) ? repoIndex.migrations_schema.length : 0;
  const cluster = openapiCount > 0 || routeCount > 0 ? "api-contract" : migrationCount > 0 ? "data-model" : "repo-core";
  const candidateSkillId = slugifyToken(`${repoId}-${cluster}`);
  const reason = `Clustered from ${scope} evidence (api=${openapiCount}, routes=${routeCount}, migrations=${migrationCount}).`;
  return { candidateSkillId, reason, evidenceRefs: evidenceRefs.slice(0, 32) };
}

async function deriveSystemDraft({ paths }) {
  const evidenceRefs = await readEvidenceIdsForSystem(paths);
  if (!evidenceRefs.length) throw new Error("No system evidence refs available. Run --knowledge-scan first.");

  const integrationMapAbs = join(paths.knowledge.viewsAbs, "integration_map.json");
  const integrationMap = await readJsonAbs(integrationMapAbs).catch(() => null);
  const edgeCount = Array.isArray(integrationMap?.edges) ? integrationMap.edges.length : 0;
  const nodeCount = Array.isArray(integrationMap?.nodes) ? integrationMap.nodes.length : 0;
  const candidateSkillId = "system-integration-guardrails";
  const reason = `Clustered from system integration view (nodes=${nodeCount}, edges=${edgeCount}).`;
  return { candidateSkillId, reason, evidenceRefs: evidenceRefs.slice(0, 32) };
}

export async function runSkillsDraft({
  projectRoot,
  scope,
  prefill = null,
  dryRun = false,
} = {}) {
  const parsedScope = validateScope(scope);
  const paths = await loadProjectPaths({ projectRoot });
  const draftsDirAbs = join(paths.laneA.skillsDirAbs, "drafts");

  let derived = null;
  if (prefill && typeof prefill === "object" && !Array.isArray(prefill)) {
    const candidateSkillId = slugifyToken(prefill.candidateSkillId || prefill.candidate_skill_id || "");
    const reason = normStr(prefill.reason);
    const evidenceRefs = Array.isArray(prefill.evidenceRefs || prefill.evidence_refs)
      ? Array.from(
          new Set(
            (prefill.evidenceRefs || prefill.evidence_refs)
              .map((value) => normStr(value))
              .filter(Boolean),
          ),
        ).sort((a, b) => a.localeCompare(b))
      : [];
    if (!candidateSkillId) throw new Error("skills-draft prefill requires candidateSkillId.");
    if (!reason) throw new Error("skills-draft prefill requires reason.");
    if (!evidenceRefs.length) throw new Error("skills-draft prefill requires evidenceRefs.");
    derived = { candidateSkillId, reason, evidenceRefs };
  } else {
    derived =
      parsedScope.kind === "repo"
        ? await deriveRepoDraft({ paths, repoId: parsedScope.repo_id, scope: parsedScope.scope })
        : await deriveSystemDraft({ paths });
  }

  const ts = nowFsSafeUtcTimestamp();
  const draftId = `DRAFT-${ts}`;
  const draft = draftShape({
    draftId,
    scope: parsedScope.scope,
    candidateSkillId: derived.candidateSkillId,
    reason: derived.reason,
    evidenceRefs: derived.evidenceRefs,
  });
  const outAbs = join(draftsDirAbs, `${draftId}.json`);

  if (!dryRun) {
    await mkdir(draftsDirAbs, { recursive: true });
    await writeFile(outAbs, jsonStableStringify(draft), "utf8");
  }

  return {
    ok: true,
    dry_run: !!dryRun,
    draft_id: draftId,
    scope: parsedScope.scope,
    path: outAbs,
    candidate_skill_id: draft.candidate_skill_id,
    evidence_refs: draft.evidence_refs,
  };
}
