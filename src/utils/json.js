function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

export function sortKeysDeep(v) {
  if (Array.isArray(v)) return v.map(sortKeysDeep);
  if (!isPlainObject(v)) return v;
  const keys = Object.keys(v).sort((a, b) => a.localeCompare(b));
  const out = {};
  for (const k of keys) out[k] = sortKeysDeep(v[k]);
  return out;
}

export function jsonStableStringify(obj, space = 2) {
  return JSON.stringify(sortKeysDeep(obj), null, space) + "\n";
}

