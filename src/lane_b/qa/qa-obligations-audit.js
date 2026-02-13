import { posix as pathPosix } from "node:path";

function normalizeRepoRelPathForCompare(p) {
  const s = String(p || "").trim();
  if (!s) return "";
  const replaced = s.replaceAll("\\\\", "/");
  const norm = pathPosix.normalize(replaced);
  if (norm === "." || norm === "./") return ".";
  return norm.startsWith("./") ? norm.slice(2) : norm;
}

export function classifyTestEditPath(pathRelRaw) {
  const p = normalizeRepoRelPathForCompare(pathRelRaw);
  const lower = p.toLowerCase();
  if (!lower || lower === ".") return null;
  if (lower.includes("/cypress/") || lower.includes("/playwright/") || lower.includes("/e2e/") || lower.startsWith("e2e/")) return "e2e";
  if (lower.includes("/integration/") || lower.includes("/itest/") || lower.includes(".int.test.")) return "integration";
  if (lower.includes("__tests__/")) return "unit";
  if (lower.startsWith("test/") || lower.includes("/test/")) return "unit";
  if (lower.startsWith("tests/") || lower.includes("/tests/")) return "unit";
  if (
    lower.endsWith(".test.js") ||
    lower.endsWith(".spec.js") ||
    lower.endsWith(".test.jsx") ||
    lower.endsWith(".spec.jsx") ||
    lower.endsWith(".test.ts") ||
    lower.endsWith(".spec.ts") ||
    lower.endsWith(".test.tsx") ||
    lower.endsWith(".spec.tsx")
  ) return "unit";
  if (lower.endsWith("_test.go") || lower.endsWith("_test.py")) return "unit";
  return null;
}

export function parseWaivedQaObligationsFromNotes(notesRaw) {
  const notes = String(notesRaw || "").toLowerCase();
  const out = { unit: false, integration: false, e2e: false };
  const has = (re) => re.test(notes);
  const waiveAll = has(/waive\s*[:=_-]?\s*all/) || has(/waive\s*[:=_-]?\s*tests\s*[:=_-]?\s*all/);
  if (waiveAll) return { unit: true, integration: true, e2e: true };
  if (has(/waive\s*[:=_-]?\s*unit/)) out.unit = true;
  if (has(/waive\s*[:=_-]?\s*integration/)) out.integration = true;
  if (has(/waive\s*[:=_-]?\s*e2e/)) out.e2e = true;
  return out;
}

export function auditQaObligationsAgainstEditPaths({ obligations, editPaths, qaApprovalStatus = "pending", qaApprovalNotes = null } = {}) {
  const o = obligations && typeof obligations === "object" ? obligations : {};
  const required = {
    unit: o.must_add_unit === true,
    integration: o.must_add_integration === true,
    e2e: o.must_add_e2e === true,
  };
  const status = String(qaApprovalStatus || "pending").trim().toLowerCase() || "pending";
  if (status === "rejected") return { ok: false, missing: ["qa_rejected"], required, present: { unit: false, integration: false, e2e: false }, waived: { unit: false, integration: false, e2e: false } };

  const waived = status === "approved" ? parseWaivedQaObligationsFromNotes(qaApprovalNotes) : { unit: false, integration: false, e2e: false };
  const present = { unit: false, integration: false, e2e: false };
  for (const p of Array.isArray(editPaths) ? editPaths : []) {
    const kind = classifyTestEditPath(p);
    if (kind === "unit") present.unit = true;
    if (kind === "integration") present.integration = true;
    if (kind === "e2e") present.e2e = true;
  }
  const missing = [];
  if (required.unit && !present.unit && !waived.unit) missing.push("unit");
  if (required.integration && !present.integration && !waived.integration) missing.push("integration");
  if (required.e2e && !present.e2e && !waived.e2e) missing.push("e2e");
  return { ok: missing.length === 0, missing, required, present, waived, qa_status: status };
}
