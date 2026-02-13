function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
}

function isHexLower(x) {
  return typeof x === "string" && /^[0-9a-f]{64}$/.test(x);
}

function normalizeStringArray(arr) {
  if (!Array.isArray(arr)) return null;
  const out = [];
  for (const v of arr) {
    const s = String(v || "").trim();
    if (!s) continue;
    if (!out.includes(s)) out.push(s);
  }
  return out;
}

export function validateSsotSnapshot(raw) {
  const errors = [];
  const add = (m) => errors.push(String(m));

  if (!isPlainObject(raw)) return { ok: false, errors: ["SSOT snapshot must be a JSON object."], normalized: null };
  if (raw.version !== 1) add("SSOT snapshot.version must be 1.");

  const project_code = isNonEmptyString(raw.project_code) ? raw.project_code.trim() : null;
  if (!project_code) add("SSOT snapshot.project_code must be a non-empty string.");

  const created_at = isNonEmptyString(raw.created_at) ? raw.created_at.trim() : null;
  if (!created_at) add("SSOT snapshot.created_at must be a non-empty ISO string.");

  const sectionsRaw = Array.isArray(raw.sections) ? raw.sections : null;
  if (!sectionsRaw) add("SSOT snapshot.sections must be an array.");

  const sections = [];
  const seenIds = new Set();
  if (sectionsRaw) {
    for (let i = 0; i < sectionsRaw.length; i += 1) {
      const s = sectionsRaw[i];
      if (!isPlainObject(s)) {
        add(`sections[${i}] must be an object.`);
        continue;
      }
      const id = isNonEmptyString(s.id) ? s.id.trim() : null;
      const path = isNonEmptyString(s.path) ? s.path.trim() : null;
      const sha256 = isHexLower(s.sha256) ? s.sha256 : null;
      if (!id) add(`sections[${i}].id must be a non-empty string.`);
      if (!path) add(`sections[${i}].path must be a non-empty string (relative to the knowledge project directory root).`);
      if (!sha256) add(`sections[${i}].sha256 must be a 64-char lowercase hex sha256.`);
      if (id) {
        if (seenIds.has(id)) add(`sections[${i}].id duplicates '${id}'.`);
        seenIds.add(id);
      }
      if (id && path && sha256) sections.push({ id, path, sha256 });
    }
  }

  const packs = normalizeStringArray(raw.shared_knowledge_packs);
  if (typeof raw.shared_knowledge_packs !== "undefined" && raw.shared_knowledge_packs !== null && !packs) {
    add("SSOT snapshot.shared_knowledge_packs must be a string[] (or omit).");
  }

  if (errors.length) return { ok: false, errors, normalized: null };
  return {
    ok: true,
    errors: [],
    normalized: {
      version: 1,
      project_code,
      created_at,
      shared_knowledge_packs: packs || [],
      sections: sections.slice().sort((a, b) => a.id.localeCompare(b.id)),
    },
  };
}
