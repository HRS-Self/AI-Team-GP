import { sha256Hex } from "../../utils/fs-hash.js";
import { gitShowFileAtRef } from "../../utils/git-files.js";
import { validateEvidenceRef } from "../../contracts/validators/index.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function uniqSorted(arr) {
  return Array.from(new Set((Array.isArray(arr) ? arr : []).map((x) => String(x || "").trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
}

function safeRepoRelativePath(p) {
  const s = normStr(p);
  if (!s) return null;
  if (s.startsWith("/") || s.includes("..") || s.includes("\\")) return null;
  return s.replace(/^\/+/, "");
}

function looksLikeRepoPath(p) {
  const s = safeRepoRelativePath(p);
  if (!s) return false;
  // Heuristic: treat as a path if it has a slash or a dot-extension or starts with a dot file.
  if (s.includes("/")) return true;
  if (s.startsWith(".")) return true;
  if (/\.[a-z0-9]+$/i.test(s)) return true;
  return false;
}

export function collectEvidenceFilePaths({ repoIndex, repoFingerprints }) {
  const entrypoints = Array.isArray(repoIndex?.entrypoints) ? repoIndex.entrypoints : [];
  const hotspots = Array.isArray(repoIndex?.hotspots) ? repoIndex.hotspots : [];
  const api = isPlainObject(repoIndex?.api_surface) ? repoIndex.api_surface : {};
  const build = isPlainObject(repoIndex?.build_commands) ? repoIndex.build_commands : {};
  const migrations = Array.isArray(repoIndex?.migrations_schema) ? repoIndex.migrations_schema : [];
  const crossDeps = Array.isArray(repoIndex?.cross_repo_dependencies) ? repoIndex.cross_repo_dependencies : [];

  const out = [];
  for (const p of entrypoints) {
    const s = safeRepoRelativePath(p);
    if (s) out.push(s);
  }
  for (const h of hotspots) {
    if (!isPlainObject(h)) continue;
    const s = safeRepoRelativePath(h.file_path);
    if (s) out.push(s);
  }

  const openapiFiles = Array.isArray(api.openapi_files) ? api.openapi_files : [];
  const routesControllers = Array.isArray(api.routes_controllers) ? api.routes_controllers : [];
  const eventsTopics = Array.isArray(api.events_topics) ? api.events_topics : [];
  for (const p of openapiFiles) {
    const s = safeRepoRelativePath(p);
    if (s) out.push(s);
  }
  for (const p of routesControllers.slice().sort((a, b) => String(a).localeCompare(String(b))).slice(0, 50)) {
    const s = safeRepoRelativePath(p);
    if (s) out.push(s);
  }
  for (const p of eventsTopics.slice().sort((a, b) => String(a).localeCompare(String(b))).slice(0, 50)) {
    const s = safeRepoRelativePath(p);
    if (s) out.push(s);
  }

  const evidenceFiles = Array.isArray(build.evidence_files) ? build.evidence_files : [];
  for (const p of evidenceFiles) {
    const s = safeRepoRelativePath(p);
    if (s) out.push(s);
  }

  for (const p of migrations.slice().sort((a, b) => String(a).localeCompare(String(b))).slice(0, 50)) {
    const s = safeRepoRelativePath(p);
    if (s) out.push(s);
  }

  for (const d of crossDeps) {
    if (!isPlainObject(d)) continue;
    const refs = Array.isArray(d.evidence_refs) ? d.evidence_refs : [];
    for (const p of refs) {
      const s = safeRepoRelativePath(p);
      if (s) out.push(s);
    }
  }

  const fpFiles = Array.isArray(repoFingerprints?.files) ? repoFingerprints.files : [];
  for (const f of fpFiles) {
    if (!isPlainObject(f)) continue;
    const s = safeRepoRelativePath(f.path);
    if (s) out.push(s);
  }

  // Also include all fingerprint keys from repo_index.fingerprints (authoritative).
  const fpMap = isPlainObject(repoIndex?.fingerprints) ? repoIndex.fingerprints : {};
  for (const p of Object.keys(fpMap)) {
    const s = safeRepoRelativePath(p);
    if (s) out.push(s);
  }

  return uniqSorted(out);
}

export function stableEvidenceId({ repo_id, commit_sha, file_path, start_line, end_line }) {
  const base = [repo_id, commit_sha, file_path, `${start_line}:${end_line}`].map((x) => String(x || "")).join("\n");
  return `EVID_${sha256Hex(base).slice(0, 12)}`;
}

export function generateEvidenceRefs({ repo_id, repo_abs, git_ref, commit_sha, captured_at_iso, file_paths, extractor = "knowledge_scan_index" }) {
  const repoId = normStr(repo_id);
  const ref = normStr(git_ref) || "HEAD";
  const sha = normStr(commit_sha);
  const capturedAt = normStr(captured_at_iso);
  const paths = uniqSorted(file_paths).map(safeRepoRelativePath).filter(Boolean);

  const refs = [];
  for (const p of paths) {
    const shown = gitShowFileAtRef(repo_abs, ref, p);
    if (!shown.ok) {
      throw new Error(`Evidence file missing at ${ref}:${p} (${shown.error})`);
    }
    const text = String(shown.content || "");
    const lines = text.split("\n");
    const end = Math.max(1, Math.min(200, lines.length || 1));
    const start = 1;
    const evidence_id = stableEvidenceId({ repo_id: repoId, commit_sha: sha, file_path: p, start_line: start, end_line: end });
    const obj = {
      evidence_id,
      repo_id: repoId,
      file_path: p,
      commit_sha: sha,
      start_line: start,
      end_line: end,
      extractor,
      captured_at: capturedAt,
    };
    validateEvidenceRef(obj);
    refs.push(obj);
  }

  refs.sort((a, b) => a.file_path.localeCompare(b.file_path));
  const jsonl = refs.map((o) => JSON.stringify(o)).join("\n") + (refs.length ? "\n" : "");
  return { refs, jsonl };
}

export function mapFactsToEvidence({ facts, evidenceRefs }) {
  const refs = Array.isArray(evidenceRefs) ? evidenceRefs : [];
  const byId = new Map(refs.map((r) => [String(r.evidence_id), r]));

  const out = [];
  for (const f of Array.isArray(facts) ? facts : []) {
    if (!isPlainObject(f)) throw new Error("fact must be an object");
    const evidence_ids = Array.isArray(f.evidence_ids) ? f.evidence_ids.map((x) => String(x || "").trim()).filter(Boolean) : [];
    if (!evidence_ids.length) throw new Error(`fact ${String(f.fact_id || "(missing)")}: evidence_ids is required`);
    for (const id of evidence_ids) {
      if (!byId.has(id)) throw new Error(`fact ${String(f.fact_id || "(missing)")}: unknown evidence_id ${id}`);
    }
    out.push({ fact_id: String(f.fact_id), claim: String(f.claim), evidence_ids });
  }
  return out;
}
