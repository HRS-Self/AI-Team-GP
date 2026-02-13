import { createHash } from "node:crypto";

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function normLower(x) {
  return normStr(x).toLowerCase();
}

function normUpper(x) {
  return normStr(x).toUpperCase();
}

function stableEvidenceKey(e) {
  const type = normLower(e?.type);
  if (type === "file") return `file:${normStr(e?.path)}`;
  if (type === "grep") return `grep:${normStr(e?.pattern)}`;
  if (type === "endpoint") return `endpoint:${normUpper(e?.method)} ${normStr(e?.path)}`;
  return `${type}:${sha256Hex(JSON.stringify(e || {}))}`;
}

export function computeStableGapId(gap) {
  const scope = normLower(gap?.scope);
  const category = normLower(gap?.category);
  const summary = normStr(gap?.summary);
  const expected = normStr(gap?.expected);
  const observed = normStr(gap?.observed);
  const evidence = Array.isArray(gap?.evidence) ? gap.evidence.filter(isPlainObject).map(stableEvidenceKey).filter(Boolean).sort() : [];
  const basis = [scope, category, summary, expected, observed, ...evidence].join("\n");
  return `GAP_${sha256Hex(basis).slice(0, 12)}`;
}

