import { existsSync } from "node:fs";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

import { sha256Hex } from "../../utils/fs-hash.js";
import { resolveGitRefForBranch, gitShowFileAtRef } from "../../utils/git-files.js";
import {
  detectApiSurface,
  detectBuildCommands,
  detectCrossRepoDependenciesFromPackageJson,
  detectEntrypoints,
  detectHotspots,
  detectLanguages,
  detectMigrationsSchema,
  selectFingerprintFiles,
} from "../../utils/repo-discovery.js";
import { validateRepoIndex } from "../../contracts/validators/index.js";

function nowISO() {
  return new Date().toISOString();
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

function git(repoAbs, args, { timeoutMs = 30_000 } = {}) {
  const safe = String(repoAbs || "").trim();
  const safeArgs = safe ? ["-c", `safe.directory=${safe}`] : [];
  const res = spawnSync("git", [...safeArgs, "-C", repoAbs, ...args], { encoding: "utf8", timeout: timeoutMs });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function isGitWorktree(repoAbs) {
  const res = git(repoAbs, ["rev-parse", "--is-inside-work-tree"]);
  return res.ok && res.stdout.trim() === "true";
}

function readJsonFromRef(repoAbs, gitRef, path) {
  const shown = gitShowFileAtRef(repoAbs, gitRef, path);
  if (!shown.ok) return { ok: false, json: null, message: `Failed to read ${path} at ${gitRef}: ${shown.error}` };
  try {
    return { ok: true, json: JSON.parse(String(shown.content || "")), message: null };
  } catch (err) {
    return { ok: false, json: null, message: `Invalid JSON in ${path} at ${gitRef}: ${err instanceof Error ? err.message : String(err)}` };
  }
}

function listRepoFilesAtRef(repoAbs, gitRef) {
  const ref = String(gitRef || "HEAD").trim() || "HEAD";
  const res = git(repoAbs, ["ls-tree", "-r", "--name-only", ref]);
  if (!res.ok) return { ok: false, files: [], message: `git ls-tree failed for ${ref}: ${res.stderr || res.stdout}` };
  const files = res.stdout
    .split("\n")
    .map((x) => String(x || "").trim())
    .filter(Boolean)
    .filter((p) => !p.startsWith("/") && !p.includes("..") && !p.includes("\\"))
    .sort((a, b) => a.localeCompare(b));
  return { ok: true, files, message: null };
}

function getRefHeadInfo(repoAbs, gitRef) {
  const ref = String(gitRef || "HEAD").trim() || "HEAD";
  const shaRes = git(repoAbs, ["rev-list", "-1", ref]);
  if (!shaRes.ok) return { ok: false, head_sha: null, scanned_at: null, message: `git rev-list failed for ${ref}: ${shaRes.stderr || shaRes.stdout}` };
  const head_sha = shaRes.stdout.trim();
  const tsRes = git(repoAbs, ["show", "-s", "--format=%ct", ref]);
  if (!tsRes.ok) return { ok: false, head_sha, scanned_at: null, message: `git show --format=%ct failed for ${ref}: ${tsRes.stderr || tsRes.stdout}` };
  const sec = Number.parseInt(tsRes.stdout.trim(), 10);
  if (!Number.isFinite(sec) || sec <= 0) return { ok: false, head_sha, scanned_at: null, message: `Invalid commit timestamp for ${ref}` };
  return { ok: true, head_sha, scanned_at: new Date(sec * 1000).toISOString(), message: null };
}

function renderRepoIndexMd({ repoIndex, repoFingerprints }) {
  const lines = [];
  lines.push(`# Repo Index: ${repoIndex.repo_id}`);
  lines.push("");
  lines.push("## Repo Overview");
  lines.push("");
  lines.push(`- repo_id: ${repoIndex.repo_id}`);
  lines.push(`- scanned_at: ${repoIndex.scanned_at}`);
  lines.push(`- head_sha: ${repoIndex.head_sha}`);
  lines.push(`- version: ${repoIndex.version}`);
  lines.push(`- languages: ${Array.isArray(repoIndex.languages) ? repoIndex.languages.join(", ") : ""}`);
  lines.push("");
  lines.push("## Entry Points");
  lines.push("");
  if (!repoIndex.entrypoints.length) lines.push("- (none)");
  for (const p of repoIndex.entrypoints) lines.push(`- ${p}`);
  lines.push("");
  lines.push("## Build Commands");
  lines.push("");
  lines.push(`- package_manager: ${repoIndex.build_commands.package_manager}`);
  if (repoIndex.build_commands.install.length) lines.push(`- install: ${repoIndex.build_commands.install.join(" | ")}`);
  if (repoIndex.build_commands.lint.length) lines.push(`- lint: ${repoIndex.build_commands.lint.join(" | ")}`);
  if (repoIndex.build_commands.build.length) lines.push(`- build: ${repoIndex.build_commands.build.join(" | ")}`);
  if (repoIndex.build_commands.test.length) lines.push(`- test: ${repoIndex.build_commands.test.join(" | ")}`);
  if (!repoIndex.build_commands.install.length && !repoIndex.build_commands.lint.length && !repoIndex.build_commands.build.length && !repoIndex.build_commands.test.length) {
    lines.push("- commands: (none)");
  }
  lines.push("");
  lines.push("## API Surface");
  lines.push("");
  const api = repoIndex.api_surface;
  if (!api.openapi_files.length && !api.routes_controllers.length && !api.events_topics.length) lines.push("- (none)");
  if (api.openapi_files.length) {
    lines.push("- openapi_files:");
    for (const p of api.openapi_files) lines.push(`  - ${p}`);
  }
  if (api.routes_controllers.length) {
    lines.push("- routes_controllers:");
    for (const p of api.routes_controllers) lines.push(`  - ${p}`);
  }
  if (api.events_topics.length) {
    lines.push("- events_topics:");
    for (const p of api.events_topics) lines.push(`  - ${p}`);
  }
  lines.push("");
  lines.push("## Migrations / Schema");
  lines.push("");
  if (!repoIndex.migrations_schema.length) lines.push("- (none)");
  for (const p of repoIndex.migrations_schema) lines.push(`- ${p}`);
  lines.push("");
  lines.push("## Cross-Repo Dependencies");
  lines.push("");
  if (!repoIndex.cross_repo_dependencies.length) lines.push("- (none)");
  for (const d of repoIndex.cross_repo_dependencies) lines.push(`- ${d.type}: ${d.target} (evidence: ${d.evidence_refs.join(", ")})`);
  lines.push("");
  lines.push("## Hotspots");
  lines.push("");
  if (!repoIndex.hotspots.length) lines.push("- (none)");
  for (const h of repoIndex.hotspots) lines.push(`- ${h.reason}: ${h.file_path}`);
  lines.push("");
  lines.push("## Fingerprint Summary");
  lines.push("");
  const byCat = new Map();
  for (const f of repoFingerprints.files) {
    const c = String(f.category || "");
    byCat.set(c, (byCat.get(c) || 0) + 1);
  }
  const cats = Array.from(byCat.keys()).sort((a, b) => a.localeCompare(b));
  for (const c of cats) lines.push(`- ${c}: ${byCat.get(c)}`);
  lines.push("");
  return lines.join("\n") + "\n";
}

function uniqSorted(arr) {
  return Array.from(new Set((Array.isArray(arr) ? arr : []).map((x) => String(x || "").trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
}

function extractUrlsFromText(text) {
  const t = String(text || "");
  const urls = [];
  const re = /\bhttps?:\/\/[^\s"'<>]+/gi;
  let m;
  // eslint-disable-next-line no-cond-assign
  while ((m = re.exec(t))) urls.push(m[0]);
  const gitRe = /\bgit@[^ \n\r\t"'<>]+/gi;
  // eslint-disable-next-line no-cond-assign
  while ((m = gitRe.exec(t))) urls.push(m[0]);
  return uniqSorted(urls).slice(0, 200);
}

function mergeCrossRepoDeps(...lists) {
  const all = [];
  for (const l of lists) for (const it of Array.isArray(l) ? l : []) all.push(it);
  const map = new Map();
  for (const d of all) {
    if (!d || typeof d !== "object") continue;
    const type = String(d.type || "").trim();
    const target = String(d.target || "").trim();
    const evidence_refs = Array.isArray(d.evidence_refs) ? d.evidence_refs.map((x) => String(x || "").trim()).filter(Boolean) : [];
    if (!type || !target || !evidence_refs.length) continue;
    const k = `${type}::${target}`;
    if (!map.has(k)) map.set(k, { type, target, evidence_refs: uniqSorted(evidence_refs) });
    else {
      const cur = map.get(k);
      cur.evidence_refs = uniqSorted([...cur.evidence_refs, ...evidence_refs]);
    }
  }
  return Array.from(map.values()).sort((a, b) => `${a.type}::${a.target}`.localeCompare(`${b.type}::${b.target}`));
}

async function clearOutputsOnFailure({ outputDirAbs }) {
  const toRemove = ["repo_index.json", "repo_index.md", "repo_fingerprints.json"];
  for (const name of toRemove) {
    try {
      // eslint-disable-next-line no-await-in-loop
      await rm(join(outputDirAbs, name), { force: true });
    } catch {
      // ignore
    }
  }
}

async function writeError({ errorDirAbs, repo_id, message, err }) {
  const p = join(errorDirAbs, `knowledge-index__${repo_id}.error.json`);
  const obj = {
    ok: false,
    repo_id,
    captured_at: nowISO(),
    message: String(message || "").trim() || "(unknown error)",
    stack: err instanceof Error ? String(err.stack || "") : String(err?.stack || ""),
  };
  await writeTextAtomic(p, JSON.stringify(obj, null, 2) + "\n");
  return p;
}

export async function runRepoIndex({ repo_id, repo_path, output_dir, error_dir_abs = null, repo_config = null, dry_run = false } = {}) {
  const repoId = String(repo_id || "").trim();
  const repoAbs = resolve(String(repo_path || ""));
  const outputDirAbs = resolve(String(output_dir || ""));
  const errorDirAbs = error_dir_abs ? resolve(String(error_dir_abs || "")) : outputDirAbs;
  if (!repoId) throw new Error("runRepoIndex: repo_id is required");
  if (!repoAbs) throw new Error("runRepoIndex: repo_path is required");
  if (!outputDirAbs) throw new Error("runRepoIndex: output_dir is required");

  try {
    if (!existsSync(repoAbs)) throw new Error(`Repo path missing: ${repoAbs}`);
    if (!isGitWorktree(repoAbs)) throw new Error(`Not a git worktree: ${repoAbs}`);

    const activeBranch = repo_config && typeof repo_config.active_branch === "string" && repo_config.active_branch.trim() ? repo_config.active_branch.trim() : null;
    const ref = activeBranch ? resolveGitRefForBranch(repoAbs, activeBranch) : null;
    const gitRef = ref || "HEAD";
    if (activeBranch && !ref) throw new Error(`active_branch not found locally: ${activeBranch}`);

    const headInfo = getRefHeadInfo(repoAbs, gitRef);
    if (!headInfo.ok) throw new Error(headInfo.message);

    const list = listRepoFilesAtRef(repoAbs, gitRef);
    if (!list.ok) throw new Error(list.message);

    const files = list.files;
    const pkg = files.includes("package.json") ? readJsonFromRef(repoAbs, gitRef, "package.json") : { ok: true, json: null, message: null };
    if (!pkg.ok) throw new Error(pkg.message);

    const entrypoints = detectEntrypoints({ repoFiles: files, packageJson: pkg.json });

    const build_commands = detectBuildCommands({ repoFiles: files, packageJson: pkg.json });
    const languages = detectLanguages({ repoFiles: files, packageJson: pkg.json, buildCommands: build_commands });
    const api_surface = detectApiSurface({ repoFiles: files });
    const migrations_schema = detectMigrationsSchema({ repoFiles: files });
    const hotspots = detectHotspots({ repoFiles: files, entrypoints, apiSurface: api_surface });

    // Cross-repo dependency discovery: deterministic, from a small set of build/config files.
    const crossFromPkg = detectCrossRepoDependenciesFromPackageJson({ packageJson: pkg.json });
    const crossFromBuildFiles = [];
    for (const p of ["pom.xml", "build.gradle", "build.gradle.kts"]) {
      if (!files.includes(p)) continue;
      const shown = gitShowFileAtRef(repoAbs, gitRef, p);
      if (!shown.ok) throw new Error(`Failed to read ${p} at ${gitRef}: ${shown.error}`);
      const urls = extractUrlsFromText(shown.content || "");
      for (const u of urls) {
        const type = u.toLowerCase().startsWith("http") ? "http" : "git";
        crossFromBuildFiles.push({ type, target: u, evidence_refs: [p] });
      }
    }
    const cross_repo_dependencies = mergeCrossRepoDeps(crossFromPkg, crossFromBuildFiles);

    const fingerprintFiles = selectFingerprintFiles({ repoFiles: files });
    if (!fingerprintFiles.length) throw new Error("No fingerprintable files detected (repo_fingerprints.files would be empty).");

    const repo_fingerprints = {
      repo_id: repoId,
      captured_at: headInfo.scanned_at,
      files: [],
    };
    const fingerprints = {};
    for (const it of fingerprintFiles) {
      const shown = gitShowFileAtRef(repoAbs, gitRef, it.path);
      if (!shown.ok) throw new Error(`Failed to read fingerprint file at ${gitRef}:${it.path}: ${shown.error}`);
      const sha256 = sha256Hex(shown.content || "");
      repo_fingerprints.files.push({ path: it.path, sha256, category: it.category });
      fingerprints[it.path] = { sha256 };
    }
    repo_fingerprints.files.sort((a, b) => `${a.category}::${a.path}`.localeCompare(`${b.category}::${b.path}`));

    const repo_index = {
      version: 1,
      repo_id: repoId,
      scanned_at: headInfo.scanned_at,
      head_sha: headInfo.head_sha,
      languages,
      entrypoints: entrypoints.slice().sort((a, b) => a.localeCompare(b)),
      hotspots: hotspots.map((h) => ({ file_path: String(h.file_path), reason: String(h.reason) })),
      build_commands,
      api_surface,
      migrations_schema,
      cross_repo_dependencies,
      fingerprints,
      dependencies: {
        version: 1,
        detected_at: headInfo.scanned_at,
        mode: "detected",
        depends_on: [],
      },
    };

    // Contract enforcement.
    validateRepoIndex(repo_index);

    // Fingerprints consistency enforcement.
    const fpKeys = Object.keys(repo_index.fingerprints).sort((a, b) => a.localeCompare(b));
    const listKeys = repo_fingerprints.files.map((f) => f.path).slice().sort((a, b) => a.localeCompare(b));
    if (fpKeys.join("\n") !== listKeys.join("\n")) throw new Error("Fingerprints mismatch between repo_index.json and repo_fingerprints.json.");
    for (const f of repo_fingerprints.files) {
      if (!repo_index.fingerprints[f.path] || repo_index.fingerprints[f.path].sha256 !== f.sha256) {
        throw new Error(`Fingerprint sha mismatch for ${f.path}`);
      }
    }

    const paths = {
      repo_index_json: join(outputDirAbs, "repo_index.json"),
      repo_index_md: join(outputDirAbs, "repo_index.md"),
      repo_fingerprints_json: join(outputDirAbs, "repo_fingerprints.json"),
    };

    if (!dry_run) {
      await mkdir(outputDirAbs, { recursive: true });
      await writeTextAtomic(paths.repo_fingerprints_json, JSON.stringify(repo_fingerprints, null, 2) + "\n");
      await writeTextAtomic(paths.repo_index_json, JSON.stringify(repo_index, null, 2) + "\n");
      await writeTextAtomic(paths.repo_index_md, renderRepoIndexMd({ repoIndex: repo_index, repoFingerprints: repo_fingerprints }));
      try {
        await rm(join(errorDirAbs, `knowledge-index__${repoId}.error.json`), { force: true });
      } catch {
        // ignore
      }
    }

    return {
      ok: true,
      repo_id: repoId,
      paths,
      git_ref: gitRef,
      head_sha: headInfo.head_sha,
      scanned_at: headInfo.scanned_at,
      repo_index,
      repo_fingerprints,
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (!dry_run) {
      await mkdir(outputDirAbs, { recursive: true });
      await mkdir(errorDirAbs, { recursive: true });
      await clearOutputsOnFailure({ outputDirAbs });
      const errorPath = await writeError({ errorDirAbs, repo_id: repoId, message: msg, err });
      return { ok: false, repo_id: repoId, message: msg, error_file: errorPath };
    }
    return { ok: false, repo_id: repoId, message: msg, error_file: null };
  }
}
