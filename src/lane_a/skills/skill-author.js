import { readFileSync } from "node:fs";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { createLlmClient } from "../../llm/client.js";
import { normalizeLlmContentToText } from "../../llm/content.js";
import { resolveLlmProfileOrError } from "../../llm/llm-profiles.js";
import { loadProjectPaths } from "../../paths/project-paths.js";
import { validateSkillPackage, validateSkillsRegistry } from "../../contracts/validators/index.js";
import { validateLlmProfiles } from "../../validators/llm-profiles-validator.js";
import { jsonStableStringify } from "../../utils/json.js";
import { nowFsSafeUtcTimestamp, sha256Hex } from "../../utils/naming.js";

const MAX_SKILL_MD_BYTES_V1 = 5 * 1024;

function nowIso() {
  return new Date().toISOString();
}

function normalizeLf(text) {
  return String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

function normStr(value) {
  return typeof value === "string" ? value.trim() : "";
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

async function readLlmProfilesFromProject({ paths }) {
  const abs = join(paths.opsConfigAbs, "LLM_PROFILES.json");
  const parsed = await readJsonAbs(abs, "LLM_PROFILES.json");
  const v = validateLlmProfiles(parsed);
  if (!v.ok) throw new Error(`LLM_PROFILES.json failed validation at ${abs}: ${v.errors.join(" | ")}`);
  return v.normalized.profiles;
}

async function loadDraftById({ draftsDirAbs, draftId }) {
  const wanted = normStr(draftId);
  if (!wanted) throw new Error("Missing --draft <draft_id>.");

  const entries = await readdir(draftsDirAbs, { withFileTypes: true }).catch(() => []);
  const files = entries.filter((entry) => entry.isFile() && entry.name.endsWith(".json")).map((entry) => join(draftsDirAbs, entry.name)).sort((a, b) => a.localeCompare(b));

  for (const abs of files) {
    // eslint-disable-next-line no-await-in-loop
    const parsed = await readJsonAbs(abs, "Skill draft");
    const id = normStr(parsed?.draft_id) || normStr(parsed?.id) || normStr(parsed?.name);
    const fileStem = normStr(abs.split("/").pop() || "").replace(/\.json$/i, "");
    if (id === wanted || fileStem === wanted) return { draft: parsed, pathAbs: abs };
  }

  throw new Error(`Draft '${wanted}' not found under ${draftsDirAbs}.`);
}

function validateDraftShape(draft) {
  if (!draft || typeof draft !== "object" || Array.isArray(draft)) throw new Error("Draft must be a JSON object.");
  if (draft.version !== 1) throw new Error("Draft version must be 1.");
  if (!normStr(draft.draft_id)) throw new Error("Draft is missing draft_id.");
  if (!normStr(draft.scope)) throw new Error("Draft is missing scope.");
  if (!normStr(draft.candidate_skill_id)) throw new Error("Draft is missing candidate_skill_id.");
  if (!Array.isArray(draft.evidence_refs) || draft.evidence_refs.length < 1) throw new Error("Draft must include evidence_refs with at least one item.");
  const status = normStr(draft.status).toLowerCase();
  if (status && status !== "pending" && status !== "published") throw new Error("Draft status must be pending|published.");
  return draft;
}

async function loadEvidenceRefsForScope({ paths, scope }) {
  const s = normStr(scope);
  const out = new Map();
  if (s === "system") {
    const reposDir = paths.knowledge.evidenceReposAbs;
    const repoEntries = await readdir(reposDir, { withFileTypes: true }).catch(() => []);
    for (const repoEntry of repoEntries.filter((entry) => entry.isDirectory()).sort((a, b) => a.name.localeCompare(b.name))) {
      const refsAbs = join(reposDir, repoEntry.name, "evidence_refs.jsonl");
      // eslint-disable-next-line no-await-in-loop
      const text = await readFile(refsAbs, "utf8").catch(() => null);
      if (!text) continue;
      const lines = String(text).split("\n").map((line) => line.trim()).filter(Boolean);
      for (const line of lines) {
        try {
          const parsed = JSON.parse(line);
          const evidenceId = normStr(parsed?.evidence_id);
          if (evidenceId) out.set(evidenceId, parsed);
        } catch {
          continue;
        }
      }
    }
    return out;
  }

  const match = s.match(/^repo:([a-z0-9._-]+)$/i);
  if (!match) throw new Error(`Invalid draft scope '${s}'. Expected system or repo:<id>.`);
  const repoId = match[1];
  const refsAbs = join(paths.knowledge.evidenceReposAbs, repoId, "evidence_refs.jsonl");
  const text = await readFile(refsAbs, "utf8").catch(() => null);
  if (!text) return out;
  const lines = String(text).split("\n").map((line) => line.trim()).filter(Boolean);
  for (const line of lines) {
    try {
      const parsed = JSON.parse(line);
      const evidenceId = normStr(parsed?.evidence_id);
      if (evidenceId) out.set(evidenceId, parsed);
    } catch {
      continue;
    }
  }
  return out;
}

function promptText() {
  const abs = resolve(process.cwd(), "src/lane_a/skills/skill-author.system.txt");
  return readFileSync(abs, "utf8");
}

async function callSkillAuthorLlm({ draft, evidenceRefs, profiles }) {
  const prof = resolveLlmProfileOrError({
    profiles,
    profileKey: "skill.author",
  });
  if (!prof.ok) throw new Error(prof.message);

  const client = createLlmClient({ ...prof.profile });
  if (!client.ok) throw new Error(client.message || "Skill Author LLM client unavailable.");

  const system = promptText();
  const userPayload = {
    version: 1,
    draft,
    evidence: evidenceRefs,
  };
  const response = await client.llm.invoke([
    { role: "system", content: system },
    { role: "user", content: JSON.stringify(userPayload) },
  ]);
  const normalized = normalizeLlmContentToText(response?.content);
  let parsed = null;
  try {
    parsed = JSON.parse(String(normalized?.text || "").trim());
  } catch {
    throw new Error("Skill Author output is not valid JSON.");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("Skill Author output must be a JSON object.");
  return parsed;
}

async function loadGlobalRegistryForWrite(aiTeamRepoRootAbs) {
  const abs = join(aiTeamRepoRootAbs, "skills", "SKILLS.json");
  const parsed = await readJsonAbs(abs, "SKILLS.json");
  validateSkillsRegistry(parsed);
  return { abs, parsed };
}

function registryEntryFromSkill({ metadata }) {
  const domainTag = normStr(metadata.domain).toLowerCase().replace(/[^a-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "");
  const scopeTag = normStr(metadata.source_scope).toLowerCase().replace(/[^a-z0-9._:-]+/g, "_");
  const tags = Array.from(new Set([domainTag, scopeTag].filter(Boolean))).sort((a, b) => a.localeCompare(b));
  return {
    skill_id: metadata.skill_id,
    title: metadata.title,
    description: `${metadata.title} (${metadata.domain})`,
    tags,
    path: `skills/${metadata.skill_id}/skill.md`,
    status: "active",
  };
}

export async function runSkillsAuthor({
  projectRoot,
  draftId,
  aiTeamRepoRoot = null,
  invokeSkillAuthorImpl = null,
  failIfSkillExists = false,
  dryRun = false,
} = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  const draftsDirAbs = join(paths.laneA.skillsDirAbs, "drafts");
  const auditDirAbs = join(paths.laneA.skillsDirAbs, "audit");

  const loadedDraft = await loadDraftById({ draftsDirAbs, draftId });
  const draft = validateDraftShape(loadedDraft.draft);
  if (normStr(draft.status).toLowerCase() === "published") throw new Error(`Draft ${draft.draft_id} is already published.`);

  const evidenceMap = await loadEvidenceRefsForScope({ paths, scope: draft.scope });
  const evidenceRefs = [];
  for (const id of draft.evidence_refs.map((entry) => normStr(entry)).filter(Boolean)) {
    const found = evidenceMap.get(id);
    if (!found) throw new Error(`Draft references unknown evidence_ref '${id}' for scope ${draft.scope}.`);
    evidenceRefs.push(found);
  }

  const aiTeamRepoRootAbs = resolveAiTeamRepoRoot(aiTeamRepoRoot);
  const profiles = await readLlmProfilesFromProject({ paths });
  const raw = invokeSkillAuthorImpl
    ? await invokeSkillAuthorImpl({ draft, evidenceRefs, profiles })
    : await callSkillAuthorLlm({ draft, evidenceRefs, profiles });

  const skillMdRaw = typeof raw?.skill_md === "string" ? raw.skill_md : "";
  const skillMd = normalizeLf(skillMdRaw);
  if (!skillMd.trim()) throw new Error("Skill Author returned empty skill_md.");
  const skillBytes = Buffer.byteLength(skillMd, "utf8");
  if (skillBytes > MAX_SKILL_MD_BYTES_V1) throw new Error(`Skill Author skill_md exceeds v1 cap (${MAX_SKILL_MD_BYTES_V1} bytes).`);

  const metadataRaw = raw && typeof raw.metadata === "object" && !Array.isArray(raw.metadata) ? { ...raw.metadata } : null;
  if (!metadataRaw) throw new Error("Skill Author returned invalid metadata.");
  const metadata = {
    ...metadataRaw,
    hash: sha256Hex(skillMd),
  };
  validateSkillPackage(metadata, { skillMd });

  const skillDirAbs = join(aiTeamRepoRootAbs, "skills", metadata.skill_id);
  const skillMdAbs = join(skillDirAbs, "skill.md");
  const skillJsonAbs = join(skillDirAbs, "skill.json");
  const { abs: registryAbs, parsed: registryBefore } = await loadGlobalRegistryForWrite(aiTeamRepoRootAbs);
  const skillExists = !!(registryBefore.skills && registryBefore.skills[metadata.skill_id]);
  if (failIfSkillExists && skillExists) throw new Error(`Skill '${metadata.skill_id}' already exists in SKILLS registry; overwrite is disabled.`);
  const registryAfter = {
    version: 1,
    updated_at: nowIso(),
    skills: {
      ...(registryBefore.skills || {}),
      [metadata.skill_id]: registryEntryFromSkill({ metadata }),
    },
  };
  validateSkillsRegistry(registryAfter);

  const ts = nowFsSafeUtcTimestamp();
  const auditAbs = join(auditDirAbs, `SKILL_CREATE_${ts}.json`);
  const draftAfter = {
    ...draft,
    status: "published",
    published_at: nowIso(),
    published_skill_id: metadata.skill_id,
    updated_at: nowIso(),
  };

  if (!dryRun) {
    await mkdir(skillDirAbs, { recursive: true });
    await mkdir(draftsDirAbs, { recursive: true });
    await mkdir(auditDirAbs, { recursive: true });
    await writeFile(skillMdAbs, `${skillMd.endsWith("\n") ? skillMd : `${skillMd}\n`}`, "utf8");
    await writeFile(skillJsonAbs, jsonStableStringify(metadata), "utf8");
    await writeFile(registryAbs, jsonStableStringify(registryAfter), "utf8");
    await writeFile(loadedDraft.pathAbs, jsonStableStringify(draftAfter), "utf8");
    await writeFile(
      auditAbs,
      jsonStableStringify({
        version: 1,
        created_at: nowIso(),
        action: "skills_author_publish",
        draft_id: draft.draft_id,
        draft_path: loadedDraft.pathAbs,
        scope: draft.scope,
        skill_id: metadata.skill_id,
        skill_dir: skillDirAbs,
        registry_path: registryAbs,
        before: registryBefore.skills && registryBefore.skills[metadata.skill_id] ? registryBefore.skills[metadata.skill_id] : null,
        after: registryAfter.skills[metadata.skill_id],
      }),
      "utf8",
    );
  }

  return {
    ok: true,
    dry_run: !!dryRun,
    draft_id: draft.draft_id,
    skill_id: metadata.skill_id,
    wrote: {
      skill_md: skillMdAbs,
      skill_json: skillJsonAbs,
      registry_json: registryAbs,
      draft_json: loadedDraft.pathAbs,
      audit_json: auditAbs,
    },
  };
}
