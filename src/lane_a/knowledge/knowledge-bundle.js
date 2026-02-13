import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, readdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { loadProjectPaths, ensureLaneADirs } from "../../paths/project-paths.js";
import { loadRepoRegistry, resolveRepoAbsPath } from "../../utils/repo-registry.js";
import { gitShowFileAtRef } from "../../utils/git-files.js";
import { validateEvidenceRef } from "../../contracts/validators/index.js";
import { validateScope } from "./knowledge-utils.js";
import { evaluateScopeStaleness, writeRefreshRequiredDecisionPacketIfNeeded } from "../lane-a-staleness-policy.js";
import { appendFile } from "../../utils/fs.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function nowISO() {
  return new Date().toISOString();
}

function sha256Hex(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function normalizeTextLf(text) {
  return String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

function isJsonExt(p) {
  return String(p || "").toLowerCase().endsWith(".json");
}

function isTextExt(p) {
  const s = String(p || "").toLowerCase();
  return (
    s.endsWith(".md") ||
    s.endsWith(".txt") ||
    s.endsWith(".jsonl") ||
    s.endsWith(".yml") ||
    s.endsWith(".yaml") ||
    s.endsWith(".graphql") ||
    s.endsWith(".proto") ||
    s.endsWith(".js") ||
    s.endsWith(".ts") ||
    s.endsWith(".tsx") ||
    s.endsWith(".jsx") ||
    s.endsWith(".css") ||
    s.endsWith(".html")
  );
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

const VOLATILE_KEYS = new Set(["generated_at", "captured_at", "scanned_at", "updated_at", "last_seen_at", "run_at", "created_at"]);
const ISO_AT_CONST = "1970-01-01T00:00:00.000Z";
const FS_SAFE_TS_CONST = "19700101_000000000";

function isScanJsonLogicalPath(logicalPath) {
  const lp = String(logicalPath || "").toLowerCase();
  return lp.endsWith("/scan.json") || lp === "scan.json";
}

function normalizeVolatileValue(key, logicalPath) {
  const k = String(key || "");
  if (k === "scanned_at") return isScanJsonLogicalPath(logicalPath) ? FS_SAFE_TS_CONST : ISO_AT_CONST;
  return ISO_AT_CONST;
}

function normalizeVolatileKeysDeep(x, { logicalPath }) {
  if (Array.isArray(x)) return x.map((v) => normalizeVolatileKeysDeep(v, { logicalPath }));
  if (!isPlainObject(x)) return x;
  const out = {};
  const keys = Object.keys(x).sort((a, b) => a.localeCompare(b));
  for (const k of keys) {
    if (VOLATILE_KEYS.has(String(k))) {
      out[k] = normalizeVolatileValue(k, logicalPath);
      continue;
    }
    out[k] = normalizeVolatileKeysDeep(x[k], { logicalPath });
  }
  return out;
}

function canonicalizeJson(obj, { logicalPath }) {
  const normalized = normalizeVolatileKeysDeep(obj, { logicalPath });
  return JSON.stringify(normalized, null, 2) + "\n";
}

let atomicCounter = 0;
async function writeTextAtomic(absPath, text) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  atomicCounter += 1;
  const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
  await writeFile(tmp, String(text || ""), "utf8");
  await rename(tmp, abs);
}

async function writeBytesAtomic(absPath, buf) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  atomicCounter += 1;
  const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
  await writeFile(tmp, buf);
  await rename(tmp, abs);
}

async function listFilesRecursive(absDir, { relPrefix = "", capFiles = 500 } = {}) {
  const root = resolve(String(absDir || ""));
  if (!existsSync(root)) return [];
  const out = [];

  async function walk(dirAbs, relBase) {
    if (out.length >= capFiles) return;
    const entries = await readdir(dirAbs, { withFileTypes: true });
    const sorted = entries.slice().sort((a, b) => a.name.localeCompare(b.name));
    for (const e of sorted) {
      if (out.length >= capFiles) return;
      if (e.name === ".git") continue;
      const abs = join(dirAbs, e.name);
      const rel = relBase ? `${relBase}/${e.name}` : e.name;
      if (e.isDirectory()) {
        // eslint-disable-next-line no-await-in-loop
        await walk(abs, rel);
      } else if (e.isFile()) {
        out.push(relPrefix ? `${relPrefix}/${rel}` : rel);
      }
    }
  }

  await walk(root, "");
  return out.sort((a, b) => a.localeCompare(b));
}

async function readAndNormalizeForBundle({ absSource, logicalPath }) {
  const raw = await readFile(absSource);
  if (isJsonExt(logicalPath)) {
    const txt = raw.toString("utf8");
    const obj = JSON.parse(txt);
    const canon = canonicalizeJson(obj, { logicalPath });
    const buf = Buffer.from(canon, "utf8");
    return { bytes: buf, sha256: sha256Hex(buf), size: buf.length };
  }
  if (isTextExt(logicalPath)) {
    const canon = normalizeTextLf(raw.toString("utf8"));
    const withNl = canon.endsWith("\n") ? canon : `${canon}\n`;
    const buf = Buffer.from(withNl, "utf8");
    return { bytes: buf, sha256: sha256Hex(buf), size: buf.length };
  }
  return { bytes: raw, sha256: sha256Hex(raw), size: raw.length };
}

function sliceLines(text, startLine, endLine) {
  const start = Math.max(1, Number.isFinite(Number(startLine)) ? Math.floor(Number(startLine)) : 1);
  const end = Math.max(start, Number.isFinite(Number(endLine)) ? Math.floor(Number(endLine)) : start);
  const lines = String(text || "").split("\n");
  const sIdx = Math.min(lines.length, Math.max(1, start)) - 1;
  const eIdx = Math.min(lines.length, Math.max(1, end));
  return lines.slice(sIdx, eIdx).join("\n").trimEnd();
}

function bundleScopeDirRel(parsedScope) {
  if (parsedScope.kind === "system") return "system";
  return join("repo", parsedScope.repo_id);
}

function gitHeadSha(cwdAbs) {
  const res = spawnSync("git", ["rev-parse", "HEAD"], { cwd: cwdAbs, encoding: "utf8" });
  if (res.status !== 0) return { ok: false, sha: null, error: String(res.stderr || res.stdout || "").trim() };
  const sha = normStr(res.stdout).split("\n")[0];
  return { ok: true, sha: sha || null, error: null };
}

function resolveRepoAbs({ reposJson, repoId }) {
  const repos = Array.isArray(reposJson?.repos) ? reposJson.repos : [];
  const found = repos.find((r) => normStr(r?.repo_id) === repoId);
  if (!found) return null;
  const relPath = normStr(found?.path);
  if (!relPath) return null;
  return resolveRepoAbsPath({ baseDir: reposJson.base_dir, repoPath: relPath });
}

function loadEvidenceRefsJsonl(text, { repoId }) {
  const lines = String(text || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  const refs = [];
  for (const l of lines) {
    const obj = JSON.parse(l);
    validateEvidenceRef(obj);
    if (normStr(obj.repo_id) !== repoId) throw new Error(`evidence_refs.jsonl repo_id mismatch (expected ${repoId}, got ${normStr(obj.repo_id) || "(missing)"})`);
    refs.push(obj);
  }
  refs.sort((a, b) => String(a.evidence_id).localeCompare(String(b.evidence_id)));
  return refs;
}

async function buildRepoEvidenceBundle({ repoAbs, refs }) {
  const out = [];
  for (const r of refs) {
    const shown = gitShowFileAtRef(repoAbs, r.commit_sha, r.file_path);
    if (!shown.ok) throw new Error(`git show failed for ${r.commit_sha}:${r.file_path} (${shown.error})`);
    out.push({
      evidence_id: String(r.evidence_id),
      file_path: String(r.file_path),
      commit_sha: String(r.commit_sha),
      start_line: r.start_line,
      end_line: r.end_line,
      excerpt: sliceLines(shown.content, r.start_line, r.end_line),
    });
  }
  out.sort((a, b) => a.evidence_id.localeCompare(b.evidence_id));
  return out;
}

async function listOpenDecisionsForScope({ decisionsDirAbs, scope }) {
  const absDir = resolve(String(decisionsDirAbs || ""));
  if (!existsSync(absDir)) return [];
  const entries = await readdir(absDir, { withFileTypes: true });
  const jsonFiles = entries
    .filter((e) => e.isFile() && e.name.startsWith("DECISION-") && e.name.endsWith(".json"))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));

  const out = [];
  for (const f of jsonFiles) {
    const abs = join(absDir, f);
    // eslint-disable-next-line no-await-in-loop
    const txt = await readFile(abs, "utf8");
    const j = JSON.parse(txt);
    if (normStr(j?.status).toLowerCase() !== "open") continue;
    if (normStr(j?.scope) !== scope) continue;
    out.push(f);
  }
  return out.sort((a, b) => a.localeCompare(b));
}

function requireWithinOpsLaneABundles({ outAbs, bundlesRootAbs }) {
  const out = resolve(String(outAbs || ""));
  const root = resolve(String(bundlesRootAbs || ""));
  if (!out || !root) throw new Error("Invalid output path.");
  if (out === root) return;
  if (out.startsWith(`${root}/`) || out.startsWith(`${root}\\`)) return;
  throw new Error(`--out must be under OPS_ROOT/ai/lane_a/bundles (got: ${out})`);
}

export async function runKnowledgeBundle({ projectRoot, scope, out = null, dryRun = false, forceStaleOverride = false, by = null, reason = null } = {}) {
  const paths = await loadProjectPaths({ projectRoot: projectRoot ? resolve(String(projectRoot)) : null });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });

  const parsedScope = validateScope(scope);
  const bundlesRootAbs = join(paths.laneA.rootAbs, "bundles");
  const scopeDirRel = bundleScopeDirRel(parsedScope);
  const scopeDirAbs = join(bundlesRootAbs, scopeDirRel);

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, message: reposRes.message };
  const reposJson = reposRes.registry;

  // Staleness guard (strict). Bundles are Lane A outputs and must refuse when stale, unless explicitly overridden.
  const overrideBy = normStr(by) || normStr(process.env.USER || "");
  const overrideReason = normStr(reason);

  {
    const st = await evaluateScopeStaleness({ paths, registry: reposJson, scope: parsedScope.kind === "repo" ? `repo:${parsedScope.repo_id}` : "system" });
    if (st.stale && !forceStaleOverride) {
      const decision = await writeRefreshRequiredDecisionPacketIfNeeded({
        paths,
        repoId: parsedScope.kind === "repo" ? parsedScope.repo_id : null,
        blockingState: "BUNDLE",
        staleInfo: { stale_reason: st.reasons[0] || "stale", stale_reasons: st.reasons, stale_repos: st.stale_repos || [] },
        producer: "bundle",
        dryRun,
      });
      return { ok: false, error: "knowledge_stale", scope: parsedScope.scope, reasons: st.reasons, decision_written: decision?.decision_id || null };
    }
    if (st.stale && forceStaleOverride) {
      await appendFile(
        "ai/lane_a/ledger.jsonl",
        JSON.stringify({ timestamp: nowISO(), type: "stale_override", command: "knowledge-bundle", scope: parsedScope.scope, by: overrideBy || null, reason: overrideReason || null }) + "\n",
      );
    }
  }

  const include = []; // { logical_path, source_abs, source_label? }

  // System SSOT selection (repo bundles: required sections; system bundles: all).
  if (parsedScope.kind === "system") {
    const sysAbs = join(paths.knowledge.rootAbs, "ssot", "system");
    const rels = await listFilesRecursive(sysAbs, { relPrefix: "ssot/system" });
    for (const rel of rels) include.push({ logical_path: rel, source_abs: join(paths.knowledge.rootAbs, rel) });
  } else {
    const sysCore = [
      "ssot/system/PROJECT_SNAPSHOT.json",
      "ssot/system/minimum.json",
      "ssot/system/integration.json",
      "ssot/system/gaps.json",
      "ssot/system/assumptions.json",
      "ssot/system/milestones.json",
    ];
    for (const rel of sysCore) {
      const abs = join(paths.knowledge.rootAbs, rel);
      if (existsSync(abs)) include.push({ logical_path: rel, source_abs: abs });
    }
    const sectionsAbs = join(paths.knowledge.rootAbs, "ssot", "system", "sections");
    const rels = await listFilesRecursive(sectionsAbs, { relPrefix: "ssot/system/sections" });
    for (const rel of rels) include.push({ logical_path: rel, source_abs: join(paths.knowledge.rootAbs, rel) });
  }

  // Scope SSOT and views.
  if (parsedScope.kind === "repo") {
    const repoId = parsedScope.repo_id;

    const ssotRepoAbs = join(paths.knowledge.rootAbs, "ssot", "repos", repoId);
    const ssotRepoRels = await listFilesRecursive(ssotRepoAbs, { relPrefix: `ssot/repos/${repoId}` });
    for (const rel of ssotRepoRels) include.push({ logical_path: rel, source_abs: join(paths.knowledge.rootAbs, rel) });

    const viewRepoAbs = join(paths.knowledge.rootAbs, "views", "repos", repoId);
    const viewRepoRels = await listFilesRecursive(viewRepoAbs, { relPrefix: `views/repos/${repoId}` });
    for (const rel of viewRepoRels) include.push({ logical_path: rel, source_abs: join(paths.knowledge.rootAbs, rel) });
  } else {
    const viewsTeamsAbs = join(paths.knowledge.rootAbs, "views", "teams");
    const viewsTeamsRels = await listFilesRecursive(viewsTeamsAbs, { relPrefix: "views/teams" });
    for (const rel of viewsTeamsRels) include.push({ logical_path: rel, source_abs: join(paths.knowledge.rootAbs, rel) });
    const viewsSystemAbs = join(paths.knowledge.rootAbs, "views", "system");
    const viewsSystemRels = await listFilesRecursive(viewsSystemAbs, { relPrefix: "views/system" });
    for (const rel of viewsSystemRels) include.push({ logical_path: rel, source_abs: join(paths.knowledge.rootAbs, rel) });
    const integrationMapAbs = join(paths.knowledge.rootAbs, "views", "integration_map.json");
    if (existsSync(integrationMapAbs)) include.push({ logical_path: "views/integration_map.json", source_abs: integrationMapAbs });
  }

  // Evidence (curated) + index.
  if (parsedScope.kind === "repo") {
    const repoId = parsedScope.repo_id;

    const idxAbs = join(paths.knowledge.rootAbs, "evidence", "index", "repos", repoId);
    const idxRels = await listFilesRecursive(idxAbs, { relPrefix: `evidence/index/repos/${repoId}` });
    for (const rel of idxRels) include.push({ logical_path: rel, source_abs: join(paths.knowledge.rootAbs, rel) });

    const evAbs = join(paths.knowledge.rootAbs, "evidence", "repos", repoId);
    const evRels = await listFilesRecursive(evAbs, { relPrefix: `evidence/repos/${repoId}` });
    for (const rel of evRels) include.push({ logical_path: rel, source_abs: join(paths.knowledge.rootAbs, rel) });
  } else {
    const evSysAbs = join(paths.knowledge.rootAbs, "evidence", "system");
    const evSysRels = await listFilesRecursive(evSysAbs, { relPrefix: "evidence/system" });
    for (const rel of evSysRels) include.push({ logical_path: rel, source_abs: join(paths.knowledge.rootAbs, rel) });

    const idxRootAbs = join(paths.knowledge.rootAbs, "evidence", "index", "repos");
    if (existsSync(idxRootAbs)) {
      const entries = await readdir(idxRootAbs, { withFileTypes: true });
      const repoDirs = entries.filter((e) => e.isDirectory()).map((e) => e.name).sort((a, b) => a.localeCompare(b));
      for (const repoId of repoDirs) {
        for (const file of ["repo_index.json", "repo_fingerprints.json", "repo_index.md"]) {
          const rel = `evidence/index/repos/${repoId}/${file}`;
          const abs = join(paths.knowledge.rootAbs, rel);
          if (existsSync(abs)) include.push({ logical_path: rel, source_abs: abs });
        }
      }
    }
  }

  // Decisions (open only).
  const decisionScope = parsedScope.kind === "repo" ? `repo:${parsedScope.repo_id}` : "system";
  const openDecisionJsonFiles = await listOpenDecisionsForScope({ decisionsDirAbs: paths.knowledge.decisionsAbs, scope: decisionScope });
  for (const f of openDecisionJsonFiles) {
    const relJson = `decisions/${f}`;
    include.push({ logical_path: relJson, source_abs: join(paths.knowledge.rootAbs, relJson) });
    const md = f.replace(/\.json$/, ".md");
    const relMd = `decisions/${md}`;
    const absMd = join(paths.knowledge.rootAbs, relMd);
    if (existsSync(absMd)) include.push({ logical_path: relMd, source_abs: absMd });
  }

  // Derived evidence excerpt bundle (repo scope only).
  let derivedFiles = [];
  if (parsedScope.kind === "repo") {
    const repoId = parsedScope.repo_id;
    const refsAbs = join(paths.knowledge.rootAbs, "evidence", "repos", repoId, "evidence_refs.jsonl");
    if (existsSync(refsAbs)) {
      const txt = await readFile(refsAbs, "utf8");
      const refs = loadEvidenceRefsJsonl(txt, { repoId });
      if (refs.length) {
        const repoAbs = resolveRepoAbs({ reposJson, repoId });
        if (!repoAbs || !existsSync(repoAbs)) throw new Error(`Repo path missing for evidence bundle (${repoId}): ${repoAbs || "(null)"}`);
        const evidence = await buildRepoEvidenceBundle({ repoAbs, refs });
        const outObj = { version: 1, repo_id: repoId, evidence };
        const canon = canonicalizeJson(outObj, { logicalPath: "bundle/evidence_bundle.json" });
        derivedFiles.push({
          logical_path: "bundle/evidence_bundle.json",
          source_label: "derived:evidence_bundle",
          bytes: Buffer.from(canon, "utf8"),
        });
      }
    }
  }

  // De-dupe includes by logical_path.
  const byLogical = new Map();
  for (const it of include) {
    const lp = normStr(it.logical_path);
    const sa = normStr(it.source_abs);
    if (!lp || !sa) continue;
    if (byLogical.has(lp)) continue;
    byLogical.set(lp, { logical_path: lp, source_abs: resolve(sa) });
  }

  const logicalPaths = Array.from(byLogical.keys()).sort((a, b) => a.localeCompare(b));
  const normalizedFiles = [];
  let totalBytes = 0;

  for (const lp of logicalPaths) {
    const it = byLogical.get(lp);
    // eslint-disable-next-line no-await-in-loop
    const norm = await readAndNormalizeForBundle({ absSource: it.source_abs, logicalPath: lp });
    normalizedFiles.push({
      logical_path: lp,
      source_path: lp, // K_ROOT-relative logical == source rel
      sha256: norm.sha256,
      bytes: norm.size,
      content: norm.bytes,
    });
    totalBytes += norm.size;
  }

  for (const d of derivedFiles.slice().sort((a, b) => a.logical_path.localeCompare(b.logical_path))) {
    const buf = d.bytes;
    normalizedFiles.push({
      logical_path: d.logical_path,
      source_path: d.source_label,
      sha256: sha256Hex(buf),
      bytes: buf.length,
      content: buf,
    });
    totalBytes += buf.length;
  }

  normalizedFiles.sort((a, b) => a.logical_path.localeCompare(b.logical_path));

  const manifest = {
    version: 1,
    scope: parsedScope.scope,
    files: normalizedFiles.map((f) => ({ logical_path: f.logical_path, source_path: f.source_path, sha256: f.sha256, bytes: f.bytes })),
  };
  const manifestText = canonicalizeJson(manifest, { logicalPath: "manifest.json" });
  const manifestSha = sha256Hex(Buffer.from(manifestText, "utf8"));
  const bundle_id = `sha256-${manifestSha}`;

  const outBaseAbs = out ? resolve(String(out)) : null;
  if (outBaseAbs) requireWithinOpsLaneABundles({ outAbs: outBaseAbs, bundlesRootAbs });
  const bundleDirAbs = join(outBaseAbs || scopeDirAbs, bundle_id);

  const head = gitHeadSha(paths.knowledge.rootAbs);
  const bundleJson = {
    version: 1,
    scope: parsedScope.scope,
    created_at: nowISO(),
    bundle_id,
    manifest_sha256: manifestSha,
    counts: { files: manifest.files.length, bytes: totalBytes },
    inputs: {
      knowledge_repo_commit: head.ok ? head.sha : "unknown",
      ops_checkpoints: {
        last_refresh: existsSync(join(paths.laneA.eventsCheckpointsAbs, "last_refresh.json")) ? "ai/lane_a/events/checkpoints/last_refresh.json" : null,
      },
    },
  };

  const bundleMdLines = [];
  bundleMdLines.push("KNOWLEDGE BUNDLE");
  bundleMdLines.push("");
  bundleMdLines.push(`scope: ${parsedScope.scope}`);
  bundleMdLines.push(`bundle_id: ${bundle_id}`);
  bundleMdLines.push(`manifest_sha256: ${manifestSha}`);
  bundleMdLines.push(`files: ${manifest.files.length}`);
  bundleMdLines.push(`bytes: ${totalBytes}`);
  bundleMdLines.push("");
  bundleMdLines.push("Included logical paths:");
  bundleMdLines.push("");
  for (const f of manifest.files.slice(0, 200)) bundleMdLines.push(`- ${f.logical_path}`);
  if (manifest.files.length > 200) bundleMdLines.push(`- ... (${manifest.files.length - 200} more)`);
  bundleMdLines.push("");

  if (!dryRun) {
    await mkdir(bundleDirAbs, { recursive: true });
    await mkdir(join(bundleDirAbs, "content"), { recursive: true });
    await writeTextAtomic(join(bundleDirAbs, "manifest.json"), manifestText);
    await writeTextAtomic(join(bundleDirAbs, "BUNDLE.json"), JSON.stringify(bundleJson, null, 2) + "\n");
    await writeTextAtomic(join(bundleDirAbs, "BUNDLE.md"), bundleMdLines.join("\n") + "\n");

    for (const f of normalizedFiles) {
      const destAbs = join(bundleDirAbs, "content", f.logical_path);
      // eslint-disable-next-line no-await-in-loop
      await writeBytesAtomic(destAbs, f.content);
    }

    const latestAbs = join(bundlesRootAbs, "LATEST.json");
    const latest = existsSync(latestAbs) ? JSON.parse(await readFile(latestAbs, "utf8")) : { version: 1, updated_at: null, latest_by_scope: {} };
    const next = {
      version: 1,
      updated_at: nowISO(),
      latest_by_scope: isPlainObject(latest?.latest_by_scope) ? { ...latest.latest_by_scope } : {},
    };
    next.latest_by_scope[parsedScope.scope] = {
      bundle_id,
      manifest_sha256: manifestSha,
      path: `ai/lane_a/bundles/${scopeDirRel}/${bundle_id}`,
    };
    await mkdir(dirname(latestAbs), { recursive: true });
    await writeTextAtomic(latestAbs, JSON.stringify(next, null, 2) + "\n");
  }

  return {
    ok: true,
    dry_run: dryRun,
    scope: parsedScope.scope,
    bundle_id,
    out_dir: bundleDirAbs,
    manifest_sha256: manifestSha,
    counts: bundleJson.counts,
  };
}
