import { assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

function assertArray(x, path) {
  if (!Array.isArray(x)) fail(path, "must be an array");
  return x;
}

export function validateStaleness(data) {
  assertPlainObject(data, "$");
  const allowed = new Set(["version", "generated_at", "repos", "system"]);
  for (const k of Object.keys(data)) if (!allowed.has(k)) fail(`$.${k}`, "unknown field");
  if (data.version !== 1) fail("$.version", "must be 1");
  assertIsoDateTimeZ(data.generated_at, "$.generated_at");

  assertPlainObject(data.repos, "$.repos");
  for (const [repoId, v] of Object.entries(data.repos)) {
    assertNonUuidString(repoId, `$.repos.${repoId}`, { minLength: 1 });
    assertPlainObject(v, `$.repos.${repoId}`);
    const allowedR = new Set(["stale", "reasons"]);
    for (const k of Object.keys(v)) if (!allowedR.has(k)) fail(`$.repos.${repoId}.${k}`, "unknown field");
    if (typeof v.stale !== "boolean") fail(`$.repos.${repoId}.stale`, "must be boolean");
    assertArray(v.reasons, `$.repos.${repoId}.reasons`);
    for (let i = 0; i < v.reasons.length; i += 1) assertNonUuidString(v.reasons[i], `$.repos.${repoId}.reasons[${i}]`, { minLength: 1 });
  }

  assertPlainObject(data.system, "$.system");
  const allowedS = new Set(["stale", "reasons"]);
  for (const k of Object.keys(data.system)) if (!allowedS.has(k)) fail(`$.system.${k}`, "unknown field");
  if (typeof data.system.stale !== "boolean") fail("$.system.stale", "must be boolean");
  assertArray(data.system.reasons, "$.system.reasons");
  for (let i = 0; i < data.system.reasons.length; i += 1) assertNonUuidString(data.system.reasons[i], `$.system.reasons[${i}]`, { minLength: 1 });

  return data;
}

