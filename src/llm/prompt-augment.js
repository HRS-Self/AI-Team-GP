import { runPromptEngineer } from "../prompt_engineer/prompt-engineer.js";
import { loadGlobalSkillsRegistry, loadProjectSkills, resolveAllowedSkillContents } from "../skills/skills-loader.js";

const MAX_INJECTED_SKILL_BYTES = 160 * 1024;

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normStr(x) {
  return typeof x === "string" ? x : "";
}

function toMessageArray(baseMessages) {
  if (!Array.isArray(baseMessages)) return [];
  return baseMessages.map((m) => (isPlainObject(m) ? { ...m } : m));
}

function appendIfNonEmpty(messages, role, value) {
  if (typeof value !== "string") return;
  if (!value.trim()) return;
  messages.push({ role, content: value });
}

function boolFromEnv(name, fallback = false) {
  const raw = normStr(process.env[name]).trim().toLowerCase();
  if (!raw) return fallback;
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
}

function normalizeCandidateSkillMetadata(list) {
  const out = [];
  if (!Array.isArray(list)) return out;
  for (const item of list) {
    if (!item || typeof item !== "object") continue;
    const skillId = normStr(item.skill_id || item.id).trim();
    if (!skillId) continue;
    const tags = Array.isArray(item.tags) ? item.tags.map((t) => normStr(t).trim()).filter(Boolean) : [];
    out.push({
      skill_id: skillId,
      title: normStr(item.title).trim(),
      description: normStr(item.description).trim(),
      tags,
    });
  }
  return out.sort((a, b) => a.skill_id.localeCompare(b.skill_id));
}

function formatSkillBlock({ skill_id, sha256, content }) {
  return `=== BEGIN SKILL: ${skill_id} @${sha256} ===\n${String(content || "")}\n=== END SKILL: ${skill_id} ===`;
}

function ensurePlanNotesArray(plan) {
  if (!isPlainObject(plan)) return [];
  if (!Array.isArray(plan.notes)) plan.notes = [];
  return plan.notes;
}

async function loadCandidateSkillsFromAllowlist({
  projectRoot,
  aiTeamRepoRoot,
  loadGlobalSkillsRegistryImpl,
  loadProjectSkillsImpl,
} = {}) {
  if (!projectRoot) return [];
  const [globalRegistry, projectSkills] = await Promise.all([
    loadGlobalSkillsRegistryImpl({ aiTeamRepoRoot }),
    loadProjectSkillsImpl({ projectRoot }),
  ]);

  const skillsMap = globalRegistry && typeof globalRegistry.skills === "object" ? globalRegistry.skills : {};
  const allowed = Array.isArray(projectSkills?.allowed_skills) ? projectSkills.allowed_skills : [];
  const ids = Array.from(new Set(allowed.map((s) => normStr(s).trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));

  const out = [];
  for (const id of ids) {
    const entry = skillsMap[id];
    if (!entry || typeof entry !== "object") {
      throw new Error(`Allowed skill '${id}' is missing from global skills registry.`);
    }
    out.push({
      skill_id: id,
      title: normStr(entry.title).trim(),
      description: normStr(entry.description).trim(),
      tags: Array.isArray(entry.tags) ? entry.tags.map((t) => normStr(t).trim()).filter(Boolean) : [],
    });
  }
  return out;
}

function buildPromptEngineerInput({ input, candidateSkills, projectRoot }) {
  const base = isPlainObject(input) ? { ...input } : {};
  if (!base.scope || typeof base.scope !== "string") base.scope = "system";
  if (!base.projectRoot && projectRoot) base.projectRoot = projectRoot;
  base.candidate_skills = candidateSkills;
  return base;
}

async function injectSelectedSkills({
  messages,
  plan,
  projectRoot,
  aiTeamRepoRoot,
  resolveAllowedSkillContentsImpl,
} = {}) {
  const selected = Array.isArray(plan?.decision?.skills_to_load) ? plan.decision.skills_to_load.map((s) => normStr(s).trim()).filter(Boolean) : [];
  if (!selected.length) return { messages, plan };
  if (!projectRoot) throw new Error("Skill injection requires projectRoot.");

  const allowedContents = await resolveAllowedSkillContentsImpl({ projectRoot, aiTeamRepoRoot });
  const byId = new Map(allowedContents.map((it) => [it.skill_id, it]));

  const blocks = [];
  let totalBytes = 0;
  const dropped = [];

  for (const id of selected) {
    const entry = byId.get(id);
    if (!entry) continue;
    const block = formatSkillBlock(entry);
    const blockBytes = Buffer.byteLength(block, "utf8");
    if (totalBytes + blockBytes > MAX_INJECTED_SKILL_BYTES) {
      dropped.push(id);
      continue;
    }
    blocks.push(block);
    totalBytes += blockBytes;
  }

  if (blocks.length) {
    messages.push({ role: "system", content: blocks.join("\n\n") });
  }
  if (dropped.length) {
    const notes = ensurePlanNotesArray(plan);
    notes.push({
      type: "warning",
      text: `Skill injection cap reached (${MAX_INJECTED_SKILL_BYTES} bytes). Dropped skills: ${dropped.join(", ")}`,
    });
  }

  return { messages, plan };
}

export async function maybeAugmentPromptWithEngineer({
  enabled,
  input,
  projectRoot = null,
  aiTeamRepoRoot = null,
  enableSkills = false,
  baseMessages,
  runPromptEngineerImpl = runPromptEngineer,
  loadGlobalSkillsRegistryImpl = loadGlobalSkillsRegistry,
  loadProjectSkillsImpl = loadProjectSkills,
  resolveAllowedSkillContentsImpl = resolveAllowedSkillContents,
} = {}) {
  const messages = toMessageArray(baseMessages);
  if (!enabled) return { messages, plan: null };

  const inputObj = isPlainObject(input) ? { ...input } : {};
  const candidateSkills = Array.isArray(inputObj.candidate_skills)
    ? normalizeCandidateSkillMetadata(inputObj.candidate_skills)
    : await loadCandidateSkillsFromAllowlist({
        projectRoot,
        aiTeamRepoRoot,
        loadGlobalSkillsRegistryImpl,
        loadProjectSkillsImpl,
      });

  const promptInput = buildPromptEngineerInput({ input: inputObj, candidateSkills, projectRoot });
  const plan = await runPromptEngineerImpl(promptInput);
  const delta = plan && plan.prompt_delta && typeof plan.prompt_delta === "object" ? plan.prompt_delta : {};

  appendIfNonEmpty(messages, "system", delta.system_append);
  appendIfNonEmpty(messages, "developer", delta.developer_append);
  appendIfNonEmpty(messages, "user", delta.user_append);

  if (!enableSkills) return { messages, plan };
  return injectSelectedSkills({
    messages,
    plan,
    projectRoot,
    aiTeamRepoRoot,
    resolveAllowedSkillContentsImpl,
  });
}

export async function maybeAugmentLlmMessagesWithSkills({
  baseMessages,
  input,
  projectRoot = null,
  aiTeamRepoRoot = null,
} = {}) {
  const enabled = boolFromEnv("ENABLE_SKILLS", false);
  return maybeAugmentPromptWithEngineer({
    enabled,
    enableSkills: enabled,
    baseMessages,
    input,
    projectRoot,
    aiTeamRepoRoot,
  });
}
