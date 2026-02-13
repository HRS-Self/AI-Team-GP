import { assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

function assertArray(x, path) {
  if (!Array.isArray(x)) fail(path, "must be an array");
  return x;
}

function isVersionLike(v) {
  return /^v\d+(\.\d+)*$/.test(String(v || "").trim());
}

export function validateKnowledgeVersion(data) {
  assertPlainObject(data, "$");
  const allowed = new Set(["version", "current", "history"]);
  for (const k of Object.keys(data)) if (!allowed.has(k)) fail(`$.${k}`, "unknown field");
  if (data.version !== 1) fail("$.version", "must be 1");
  const current = assertNonUuidString(data.current, "$.current", { minLength: 2 });
  if (!isVersionLike(current)) fail("$.current", "must match /^v\\d+(\\.\\d+)*$/");

  assertArray(data.history, "$.history");
  for (let i = 0; i < data.history.length; i += 1) {
    const h = data.history[i];
    assertPlainObject(h, `$.history[${i}]`);
    const allowedH = new Set(["v", "at", "reason", "scope", "notes"]);
    for (const k of Object.keys(h)) if (!allowedH.has(k)) fail(`$.history[${i}].${k}`, "unknown field");
    const v = assertNonUuidString(h.v, `$.history[${i}].v`, { minLength: 2 });
    if (!isVersionLike(v)) fail(`$.history[${i}].v`, "must match /^v\\d+(\\.\\d+)*$/");
    assertIsoDateTimeZ(h.at, `$.history[${i}].at`);
    assertNonUuidString(h.reason, `$.history[${i}].reason`, { minLength: 1 });
    assertNonUuidString(h.scope, `$.history[${i}].scope`, { minLength: 1 });
    if (h.notes !== undefined) assertNonUuidString(h.notes, `$.history[${i}].notes`, { minLength: 0 });
  }
  return data;
}

