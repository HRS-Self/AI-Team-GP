import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { createAgent as createDeepAgent } from "langchain";

import { validatePromptEngineerPlan as validatePromptEngineerPlanContract } from "../contracts/validators/validate-prompt-engineer-plan.js";
import { normalizeLlmContentToText } from "../llm/content.js";
import { createLlmClient } from "../llm/client.js";
import { loadLlmProfiles } from "../llm/llm-profiles.js";

const PROMPT_ENGINEER_PROFILE_KEY = "prompt.engineer";
const PROMPT_ENGINEER_ROLE = "prompt_engineer";
const DEFAULT_SYSTEM_PROMPT_PATH = "src/prompts/prompt-engineer.system.txt";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normString(value) {
  return typeof value === "string" ? value : "";
}

function normalizeScope(scope) {
  const s = normString(scope).trim();
  if (!s) return "system";
  return s;
}

function normalizeStringArray(value) {
  if (!Array.isArray(value)) return [];
  const out = [];
  for (const item of value) {
    const s = normString(item).trim();
    if (s) out.push(s);
  }
  return out;
}

function normalizeCandidateSkills(value) {
  if (!Array.isArray(value)) return [];
  const out = [];
  for (const item of value) {
    if (typeof item === "string") {
      const id = normString(item).trim();
      if (!id) continue;
      out.push({ skill_id: id });
      continue;
    }
    if (isPlainObject(item)) {
      const id = normString(item.skill_id || item.id).trim();
      if (!id) continue;
      const tags = Array.isArray(item.tags) ? item.tags.map((t) => normString(t).trim()).filter(Boolean) : [];
      out.push({
        skill_id: id,
        title: normString(item.title),
        description: normString(item.description),
        tags,
      });
    }
  }
  return out;
}

function normalizeObject(value) {
  return isPlainObject(value) ? value : {};
}

function resolveProfilesMap(profiles) {
  if (isPlainObject(profiles) && isPlainObject(profiles.profiles)) return profiles.profiles;
  if (isPlainObject(profiles)) return profiles;
  return null;
}

function resolvePromptEngineerProfile(profiles) {
  const map = resolveProfilesMap(profiles);
  if (!map) throw new Error("Missing LLM profiles map for Prompt Engineer.");
  const profileRaw = map[PROMPT_ENGINEER_PROFILE_KEY];
  if (!isPlainObject(profileRaw)) throw new Error(`Missing '${PROMPT_ENGINEER_PROFILE_KEY}' profile in LLM profiles.`);
  const provider = normString(profileRaw.provider).trim();
  const model = normString(profileRaw.model).trim();
  if (!provider || !model) throw new Error(`Invalid '${PROMPT_ENGINEER_PROFILE_KEY}' profile: provider/model are required.`);
  return { ...profileRaw, provider, model };
}

function resolveSystemPrompt(env) {
  const fromEnv = normString(env?.systemPromptText);
  if (fromEnv) return fromEnv;
  const promptPath = normString(env?.systemPromptPath).trim() || DEFAULT_SYSTEM_PROMPT_PATH;
  const promptAbs = resolve(process.cwd(), promptPath);
  return readFileSync(promptAbs, "utf8");
}

function buildInputPayload({
  projectRoot,
  scope,
  base_prompt,
  base_system,
  context,
  constraints,
  knowledge_snippets,
  candidate_skills,
} = {}) {
  return {
    version: 1,
    role: PROMPT_ENGINEER_ROLE,
    project_root: normString(projectRoot).trim() || null,
    scope: normalizeScope(scope),
    base_prompt: normString(base_prompt),
    base_system: normString(base_system),
    context: normalizeObject(context),
    constraints: normalizeObject(constraints),
    knowledge_snippets: normalizeStringArray(knowledge_snippets),
    candidate_skills: normalizeCandidateSkills(candidate_skills),
  };
}

function extractCandidateSkillIds(candidateSkills) {
  const ids = new Set();
  for (const item of normalizeCandidateSkills(candidateSkills)) {
    const id = normString(item.skill_id).trim();
    if (id) ids.add(id);
  }
  return ids;
}

function assertSelectedSkillsSubset(plan, candidateSkills) {
  const candidateIds = extractCandidateSkillIds(candidateSkills);
  const selected = Array.isArray(plan?.decision?.skills_to_load) ? plan.decision.skills_to_load : [];
  for (const raw of selected) {
    const id = normString(raw).trim();
    if (!id) continue;
    if (!candidateIds.has(id)) {
      throw new Error(`Prompt Engineer selected disallowed skill_id '${id}' (not present in candidate_skills).`);
    }
  }
}

function extractAgentText(raw) {
  if (typeof raw === "string") return raw;
  if (isPlainObject(raw) && typeof raw.output_text === "string") return raw.output_text;
  if (isPlainObject(raw) && typeof raw.text === "string") return raw.text;
  if (isPlainObject(raw) && typeof raw.content === "string") return raw.content;

  const messageArrays = [];
  if (isPlainObject(raw) && Array.isArray(raw.messages)) messageArrays.push(raw.messages);
  if (isPlainObject(raw) && isPlainObject(raw.output) && Array.isArray(raw.output.messages)) messageArrays.push(raw.output.messages);
  if (isPlainObject(raw) && Array.isArray(raw.response_messages)) messageArrays.push(raw.response_messages);

  for (const arr of messageArrays) {
    for (let i = arr.length - 1; i >= 0; i -= 1) {
      const msg = arr[i];
      const content = isPlainObject(msg) && "content" in msg ? msg.content : msg;
      const normalized = normalizeLlmContentToText(content);
      const text = normString(normalized?.text).trim();
      if (text) return text;
    }
  }

  return "";
}

function parseStrictJsonObject(text) {
  const t = normString(text).trim();
  if (!t) throw new Error("Prompt Engineer returned empty output.");
  if (!t.startsWith("{") || !t.endsWith("}")) throw new Error("Prompt Engineer must return JSON object only.");

  let parsed;
  try {
    parsed = JSON.parse(t);
  } catch {
    throw new Error("Prompt Engineer output is not valid JSON.");
  }
  if (!isPlainObject(parsed)) throw new Error("Prompt Engineer output must be a JSON object.");
  return parsed;
}

export function validatePromptEngineerPlan(plan) {
  return validatePromptEngineerPlanContract(plan);
}

export function buildPromptEngineerAgent({ profiles, env = {} } = {}) {
  const createLlmClientImpl = typeof env.createLlmClient === "function" ? env.createLlmClient : createLlmClient;
  const createDeepAgentImpl = typeof env.createDeepAgent === "function" ? env.createDeepAgent : createDeepAgent;
  const runDeepAgentImpl =
    typeof env.runDeepAgent === "function"
      ? env.runDeepAgent
      : async ({ agent, payload }) => agent.invoke({ messages: [{ role: "user", content: JSON.stringify(payload) }] });

  const profile = resolvePromptEngineerProfile(profiles);
  const systemPrompt = resolveSystemPrompt(env);

  const client = createLlmClientImpl({ ...profile });
  if (!client?.ok) throw new Error(normString(client?.message) || "Failed to initialize Prompt Engineer LLM client.");
  const llm = client.llm;
  if (!llm) throw new Error("Prompt Engineer LLM client returned no model instance.");

  let deepAgent;
  try {
    deepAgent = createDeepAgentImpl({ model: llm, tools: [], systemPrompt });
  } catch {
    deepAgent = createDeepAgentImpl({ model: llm, tools: [], prompt: systemPrompt });
  }
  if (!deepAgent || typeof deepAgent.invoke !== "function") throw new Error("Prompt Engineer deep agent initialization failed.");

  return {
    async run(input) {
      const payload = buildInputPayload(input);
      const raw = await runDeepAgentImpl({ agent: deepAgent, payload });
      if (isPlainObject(raw) && raw.version === 1 && raw.role === PROMPT_ENGINEER_ROLE) {
        return validatePromptEngineerPlan(raw);
      }
      const text = extractAgentText(raw);
      const parsed = parseStrictJsonObject(text);
      return validatePromptEngineerPlan(parsed);
    },
  };
}

export async function runPromptEngineer({
  projectRoot,
  scope,
  base_prompt,
  base_system,
  context,
  constraints,
  knowledge_snippets,
  candidate_skills,
  profiles = null,
  env = {},
} = {}) {
  let useProfiles = profiles;
  if (!useProfiles) {
    const loaded = await loadLlmProfiles();
    if (!loaded.ok) throw new Error(loaded.message || "Failed to load LLM profiles.");
    useProfiles = { profiles: loaded.profiles };
  }

  const agent = buildPromptEngineerAgent({ profiles: useProfiles, env });
  const plan = await agent.run({
    projectRoot,
    scope,
    base_prompt,
    base_system,
    context,
    constraints,
    knowledge_snippets,
    candidate_skills,
  });
  assertSelectedSkillsSubset(plan, candidate_skills);
  return plan;
}
