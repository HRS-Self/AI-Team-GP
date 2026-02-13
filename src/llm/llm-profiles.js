import { readTextIfExists } from "../utils/fs.js";
import { validateLlmProfiles } from "../validators/llm-profiles-validator.js";

export async function loadLlmProfiles() {
  const path = "config/LLM_PROFILES.json";
  const text = await readTextIfExists(path);
  if (!text) return { ok: false, message: `LLM_PROFILES.json missing at ${path}. Create it from template or rerun --initial-project.`, path };

  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { ok: false, message: `LLM_PROFILES.json is invalid JSON at ${path}.`, path };
  }

  const v = validateLlmProfiles(parsed);
  if (!v.ok) return { ok: false, message: `LLM_PROFILES.json failed validation at ${path}.`, path, errors: v.errors };

  return { ok: true, path, profiles: v.normalized.profiles };
}

export function resolveLlmProfileOrError({ profiles, profileKey }) {
  const key = String(profileKey || "").trim();
  if (!key) return { ok: false, message: "Agent is missing llm_profile." };

  const p = profiles && typeof profiles === "object" ? profiles[key] : null;
  if (!p) return { ok: false, message: `Unknown llm_profile '${key}'. Add it to config/LLM_PROFILES.json.` };

  const provider = String(p.provider || "").trim();
  const model = String(p.model || "").trim();
  if (!provider || !model) return { ok: false, message: `Invalid llm_profile '${key}': provider/model must be non-empty.` };

  return { ok: true, profile_key: key, profile: { ...p, provider, model } };
}

