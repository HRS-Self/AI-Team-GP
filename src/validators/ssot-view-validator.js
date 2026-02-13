function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
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

export function validateSsotView(raw) {
  const errors = [];
  const add = (m) => errors.push(String(m));

  if (!isPlainObject(raw)) return { ok: false, errors: ["SSOT view must be a JSON object."], normalized: null };
  if (raw.version !== 1) add("SSOT view.version must be 1.");

  const view_id = isNonEmptyString(raw.view_id) ? raw.view_id.trim() : null;
  if (!view_id) add("SSOT view.view_id must be a non-empty string.");

  const section_ids = normalizeStringArray(raw.section_ids);
  if (!section_ids) add("SSOT view.section_ids must be a string[]. Use [] for an empty view.");

  if (errors.length) return { ok: false, errors, normalized: null };
  return { ok: true, errors: [], normalized: { version: 1, view_id, section_ids: section_ids || [] } };
}

