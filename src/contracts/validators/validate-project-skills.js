import { fail } from "./error.js";
import { assertArray, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";

const SKILL_ID_RE = /^[a-z0-9][a-z0-9._-]{2,80}$/;

function assertSkillId(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 3 });
  if (!SKILL_ID_RE.test(s)) fail(path, "must match /^[a-z0-9][a-z0-9._-]{2,80}$/");
  return s;
}

function assertSha256(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 64 });
  if (!/^[a-f0-9]{64}$/.test(s)) fail(path, "must be 64-char lowercase hex sha256");
  return s;
}

export function validateProjectSkills(data) {
  assertPlainObject(data, "$");
  const allowedTop = new Set(["version", "project_code", "updated_at", "allowed_skills", "pinned"]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  assertNonUuidString(data.project_code, "$.project_code", { minLength: 1 });
  assertIsoDateTimeZ(data.updated_at, "$.updated_at");

  assertArray(data.allowed_skills, "$.allowed_skills");
  const allowedSet = new Set();
  for (let i = 0; i < data.allowed_skills.length; i += 1) {
    const id = assertSkillId(data.allowed_skills[i], `$.allowed_skills[${i}]`);
    if (allowedSet.has(id)) fail(`$.allowed_skills[${i}]`, "duplicate skill_id");
    allowedSet.add(id);
  }

  if (typeof data.pinned !== "undefined") {
    assertPlainObject(data.pinned, "$.pinned");
    for (const [rawId, rawPin] of Object.entries(data.pinned)) {
      const id = assertSkillId(rawId, `$.pinned['${rawId}'] (map key)`);
      if (!allowedSet.has(id)) fail(`$.pinned['${rawId}']`, "pinned skill must also exist in allowed_skills");

      assertPlainObject(rawPin, `$.pinned['${rawId}']`);
      const allowed = new Set(["content_sha256", "note"]);
      for (const k of Object.keys(rawPin)) if (!allowed.has(k)) fail(`$.pinned['${rawId}'].${k}`, "unknown field");
      assertSha256(rawPin.content_sha256, `$.pinned['${rawId}'].content_sha256`);
      if (typeof rawPin.note !== "undefined" && typeof rawPin.note !== "string") fail(`$.pinned['${rawId}'].note`, "must be a string when provided");
    }
  }

  return data;
}
