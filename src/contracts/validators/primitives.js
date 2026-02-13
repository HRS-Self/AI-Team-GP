import { fail } from "./error.js";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

export function assertPlainObject(value, path) {
  if (!isPlainObject(value)) fail(path, "must be an object");
}

export function assertBoolean(value, path) {
  if (typeof value !== "boolean") fail(path, "must be a boolean");
}

export function assertNumber(value, path) {
  if (typeof value !== "number" || !Number.isFinite(value)) fail(path, "must be a finite number");
}

export function assertInt(value, path, { min = null, max = null } = {}) {
  if (!Number.isInteger(value)) fail(path, "must be an integer");
  if (Number.isFinite(min) && value < min) fail(path, `must be >= ${min}`);
  if (Number.isFinite(max) && value > max) fail(path, `must be <= ${max}`);
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function assertNonUuidString(value, path, { minLength = 1 } = {}) {
  if (typeof value !== "string") fail(path, "must be a string");
  const s = value.trim();
  if (s.length < minLength) fail(path, `must be a non-empty string (minLength=${minLength})`);
  if (UUID_RE.test(s)) fail(path, "must not be a UUID");
  return s;
}

export function assertEnumString(value, path, allowed) {
  const s = assertNonUuidString(value, path, { minLength: 1 });
  const set = new Set(Array.isArray(allowed) ? allowed : []);
  if (!set.has(s)) fail(path, `must be one of: ${Array.from(set).join(", ")}`);
  return s;
}

export function assertIsoDateTimeZ(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 1 });
  if (!s.endsWith("Z")) fail(path, "must be an ISO timestamp ending with 'Z'");
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) fail(path, "must be a valid ISO timestamp");
  // Ensure it round-trips (rejects weird-but-parseable strings).
  const rt = new Date(ms).toISOString();
  if (rt !== s) fail(path, "must be a canonical ISO timestamp (Date.toISOString())");
  return s;
}

export function assertFsSafeUtcTimestamp(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 1 });
  if (!/^[0-9]{8}_[0-9]{9}$/.test(s)) fail(path, "must be a fs-safe UTC timestamp (YYYYMMDD_HHMMSSmmm)");
  return s;
}

export function assertSha40(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 40 });
  if (!/^[a-f0-9]{40}$/.test(s)) fail(path, "must be a 40-char lowercase hex SHA");
  return s;
}

export function assertHex64(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 64 });
  if (!/^[a-f0-9]{64}$/.test(s)) fail(path, "must be a 64-char lowercase hex string");
  return s;
}

export function assertArray(value, path, { minItems = 0 } = {}) {
  if (!Array.isArray(value)) fail(path, "must be an array");
  if (Number.isFinite(minItems) && value.length < minItems) fail(path, `must have at least ${minItems} item(s)`);
}

export function assertRelativeRepoPath(value, path) {
  const s = assertNonUuidString(value, path, { minLength: 1 });
  if (s.startsWith("/") || /^[A-Za-z]:[\\/]/.test(s)) fail(path, "must be a repo-relative path");
  if (s.includes("..")) fail(path, "must not contain '..' path traversal");
  if (s.includes("\\")) fail(path, "must use forward slashes");
  return s;
}
