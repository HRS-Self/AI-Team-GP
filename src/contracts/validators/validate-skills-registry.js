import { fail } from "./error.js";
import { assertArray, assertEnumString, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";

const SKILL_ID_RE = /^[a-z0-9][a-z0-9._-]{2,80}$/;

function assertSkillId(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 3 });
  if (!SKILL_ID_RE.test(s)) fail(path, "must match /^[a-z0-9][a-z0-9._-]{2,80}$/");
  return s;
}

function assertRelativeSkillPath(pathValue, expectedSkillId, path) {
  const p = assertNonUuidString(pathValue, path, { minLength: 1 });
  if (p.startsWith("/") || /^[A-Za-z]:[\\/]/.test(p)) fail(path, "must be relative (not absolute)");
  if (p.includes("..")) fail(path, "must not contain '..'");
  if (p.includes("\\")) fail(path, "must use forward slashes");
  const expected = `skills/${expectedSkillId}/skill.md`;
  if (p !== expected) fail(path, `must equal '${expected}'`);
  return p;
}

export function validateSkillsRegistry(data) {
  assertPlainObject(data, "$");
  const allowedTop = new Set(["version", "updated_at", "skills"]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  assertIsoDateTimeZ(data.updated_at, "$.updated_at");

  assertPlainObject(data.skills, "$.skills");
  for (const [mapKey, raw] of Object.entries(data.skills)) {
    const keyPath = `$.skills['${mapKey}']`;
    const mapSkillId = assertSkillId(mapKey, `${keyPath} (map key)`);
    assertPlainObject(raw, keyPath);

    const allowed = new Set(["skill_id", "title", "description", "tags", "path", "status"]);
    for (const k of Object.keys(raw)) if (!allowed.has(k)) fail(`${keyPath}.${k}`, "unknown field");

    const skillId = assertSkillId(raw.skill_id, `${keyPath}.skill_id`);
    if (skillId !== mapSkillId) fail(`${keyPath}.skill_id`, "must equal its object key in skills map");
    assertNonUuidString(raw.title, `${keyPath}.title`, { minLength: 1 });
    assertNonUuidString(raw.description, `${keyPath}.description`, { minLength: 1 });
    assertArray(raw.tags, `${keyPath}.tags`);
    for (let i = 0; i < raw.tags.length; i += 1) assertNonUuidString(raw.tags[i], `${keyPath}.tags[${i}]`, { minLength: 1 });
    assertRelativeSkillPath(raw.path, skillId, `${keyPath}.path`);
    assertEnumString(raw.status, `${keyPath}.status`, ["active", "deprecated"]);
  }

  return data;
}
