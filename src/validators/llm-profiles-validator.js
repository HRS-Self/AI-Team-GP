function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

export function validateLlmProfiles(raw) {
  const errors = [];

  if (!isPlainObject(raw)) return { ok: false, errors: ["LLM_PROFILES.json must be a JSON object."], normalized: null };
  if (raw.version !== 1) errors.push("LLM_PROFILES.json.version must be 1.");

  const profiles = isPlainObject(raw.profiles) ? raw.profiles : null;
  if (!profiles) errors.push("LLM_PROFILES.json.profiles must be an object mapping profile_key -> {provider, model, ...}.");

  const normalized = { version: 1, profiles: {} };
  if (profiles) {
    for (const [kRaw, vRaw] of Object.entries(profiles)) {
      const key = String(kRaw || "").trim();
      if (!key) continue;
      if (!isPlainObject(vRaw)) {
        errors.push(`profiles['${key}'] must be an object.`);
        continue;
      }
      const provider = String(vRaw.provider || "").trim();
      const model = String(vRaw.model || "").trim();
      if (!provider) errors.push(`profiles['${key}'].provider must be a non-empty string.`);
      if (!model) errors.push(`profiles['${key}'].model must be a non-empty string.`);

      // Keep only known keys; everything else is passed through as-is (for future providers).
      normalized.profiles[key] = { ...vRaw, provider: provider || null, model: model || null };
    }
  }

  return { ok: errors.length === 0, errors, normalized: errors.length === 0 ? normalized : null };
}

