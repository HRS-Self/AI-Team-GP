import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

export function hasRg() {
  const res = spawnSync("rg", ["--version"], { encoding: "utf8" });
  return res.status === 0;
}

function runGit(cwd, args) {
  const res = spawnSync("git", Array.isArray(args) ? args : [], { encoding: "utf8", cwd });
  return {
    ok: res.status === 0,
    status: typeof res.status === "number" ? res.status : null,
    stdout: String(res.stdout || ""),
    stderr: String(res.stderr || ""),
  };
}

function safeRepoId(repoId) {
  const v = String(repoId || "").trim();
  return v || "unknown-repo";
}

function parseRgLine(line) {
  const m = String(line || "").match(/^(.+?):(\d+):(.*)$/);
  if (!m) return null;
  return { path: m[1], line: m[2], text: (m[3] || "").trim() };
}

export function scanWithRgInRoots({
  repoRoots,
  terms,
  maxCountPerTerm = 60,
  maxFiles = 10,
  maxLinesPerFile = 2,
  maxEvidenceLines = 25,
}) {
  const roots = Array.isArray(repoRoots) ? repoRoots.filter((r) => r && r.abs_path) : [];
  const hitsByFile = new Map();
  let total = 0;

  const commonArgs = [
    "-n",
    "-i",
    "--no-heading",
    "--hidden",
    "--glob",
    "!**/node_modules/**",
    "--glob",
    "!**/.git/**",
    "--fixed-strings",
    "--max-count",
    String(Math.max(1, maxCountPerTerm)),
  ];

  for (const root of roots) {
    if (!root.exists) continue;
    const cwd = root.abs_path;
    const rid = safeRepoId(root.repo_id);

    for (const term of terms || []) {
      const t = String(term || "").trim();
      if (!t) continue;
      const res = spawnSync("rg", [...commonArgs, t], { encoding: "utf8", cwd });
      const out = String(res.stdout || "");
      if (!out.trim()) continue;

      const lines = out.split("\n").filter(Boolean);
      total += lines.length;
      for (const rawLine of lines) {
        const parsed = parseRgLine(rawLine);
        if (!parsed) continue;
        const key = `${rid}/${parsed.path}`;
        const entry = hitsByFile.get(key) || [];
        if (entry.length >= maxLinesPerFile) continue;
        entry.push(`L${parsed.line}: ${parsed.text}`);
        hitsByFile.set(key, entry);
        if (hitsByFile.size >= 60) break;
      }
    }
  }

  const files = Array.from(hitsByFile.entries())
    .map(([path, lines]) => ({ path, lines }))
    .sort((a, b) => b.lines.length - a.lines.length || a.path.localeCompare(b.path));

  const capped = [];
  let evidenceLines = 0;
  for (const f of files) {
    if (capped.length >= maxFiles) break;
    const keep = [];
    for (const l of f.lines) {
      if (keep.length >= maxLinesPerFile) break;
      if (evidenceLines >= maxEvidenceLines) break;
      keep.push(l);
      evidenceLines += 1;
    }
    if (keep.length) capped.push({ path: f.path, lines: keep });
    if (evidenceLines >= maxEvidenceLines) break;
  }

  return { total_matches: total, hits: capped };
}

export function scanWithGitGrepInRoots({
  repoRoots,
  terms,
  maxCountPerTerm = 60,
  maxFiles = 10,
  maxLinesPerFile = 2,
  maxEvidenceLines = 25,
}) {
  const roots = Array.isArray(repoRoots) ? repoRoots.filter((r) => r && r.abs_path && r.git_ref) : [];
  const hitsByFile = new Map();
  let total = 0;

  const commonArgs = ["grep", "-n", "-i", "--fixed-strings", "--max-count", String(Math.max(1, maxCountPerTerm))];

  for (const root of roots) {
    if (!root.exists) continue;
    const cwd = root.abs_path;
    const rid = safeRepoId(root.repo_id);
    const ref = String(root.git_ref || "").trim();
    if (!ref) continue;

    for (const term of terms || []) {
      const t = String(term || "").trim();
      if (!t) continue;
      const res = runGit(cwd, [...commonArgs, t, ref]);
      const out = String(res.stdout || "");
      if (!out.trim()) continue;

      const lines = out.split("\n").filter(Boolean);
      total += lines.length;
      for (const rawLine of lines) {
        const parsed = parseRgLine(rawLine);
        if (!parsed) continue;
        const key = `${rid}/${parsed.path}`;
        const entry = hitsByFile.get(key) || [];
        if (entry.length >= maxLinesPerFile) continue;
        entry.push(`L${parsed.line}: ${parsed.text}`);
        hitsByFile.set(key, entry);
        if (hitsByFile.size >= 60) break;
      }
    }
  }

  const files = Array.from(hitsByFile.entries())
    .map(([path, lines]) => ({ path, lines }))
    .sort((a, b) => b.lines.length - a.lines.length || a.path.localeCompare(b.path));

  const capped = [];
  let evidenceLines = 0;
  for (const f of files) {
    if (capped.length >= maxFiles) break;
    const keep = [];
    for (const l of f.lines) {
      if (keep.length >= maxLinesPerFile) break;
      if (evidenceLines >= maxEvidenceLines) break;
      keep.push(l);
      evidenceLines += 1;
    }
    if (keep.length) capped.push({ path: f.path, lines: keep });
    if (evidenceLines >= maxEvidenceLines) break;
  }

  return { total_matches: total, hits: capped };
}

async function readTextAbs(path) {
  try {
    const buf = await readFile(path);
    return buf.toString("utf8");
  } catch {
    return null;
  }
}

async function readTextAtRef({ repoAbs, gitRef, repoRelPath }) {
  const ref = String(gitRef || "").trim();
  const rel = String(repoRelPath || "").trim().replace(/^\/+/, "");
  if (!ref || !rel) return null;
  const res = runGit(repoAbs, ["show", `${ref}:${rel}`]);
  if (!res.ok) return null;
  return String(res.stdout || "");
}

export async function scanFallbackInRoots({ repoRoots, terms, maxFiles = 10, maxLinesPerFile = 2, maxEvidenceLines = 25 }) {
  const roots = Array.isArray(repoRoots) ? repoRoots.filter((r) => r && r.abs_path) : [];
  const candidates = ["package.json", "README.md"];
  const hits = [];
  let total = 0;
  let evidenceLines = 0;

  for (const root of roots) {
    if (!root.exists) continue;
    const rid = safeRepoId(root.repo_id);

    for (const rel of candidates) {
      const text = root.git_ref
        ? await readTextAtRef({ repoAbs: root.abs_path, gitRef: root.git_ref, repoRelPath: rel })
        : await readTextAbs(resolve(root.abs_path, rel));
      if (!text) continue;
      const lower = text.toLowerCase();

      const matched = [];
      for (const term of terms || []) {
        const t = String(term || "").trim().toLowerCase();
        if (!t) continue;
        if (lower.includes(t)) {
          total += 1;
          const line = text.split("\n").find((l) => l.toLowerCase().includes(t)) || "";
          matched.push(`L?: ${line.trim()}`);
        }
        if (matched.length >= maxLinesPerFile) break;
        if (evidenceLines >= maxEvidenceLines) break;
      }

      if (matched.length) {
        hits.push({ path: `${rid}/${rel}`, lines: matched.slice(0, maxLinesPerFile) });
        evidenceLines += matched.length;
      }
      if (hits.length >= maxFiles || evidenceLines >= maxEvidenceLines) break;
    }
    if (hits.length >= maxFiles || evidenceLines >= maxEvidenceLines) break;
  }

  return { total_matches: total, hits };
}

export async function discoverPackageScriptsInRoots({ repoRoots, maxPackageFilesPerRoot = 30 }) {
  const roots = Array.isArray(repoRoots) ? repoRoots.filter((r) => r && r.abs_path) : [];
  const scriptsOut = [];

  for (const root of roots) {
    if (!root.exists) continue;
    const rid = safeRepoId(root.repo_id);
    const gitRef = root.git_ref ? String(root.git_ref || "").trim() : null;

    const packageFiles = [];
    if (gitRef) {
      const listed = runGit(root.abs_path, ["ls-tree", "-r", "--name-only", gitRef]);
      if (listed.ok) {
        for (const line of String(listed.stdout || "").split("\n")) {
          const p = line.trim();
          if (!p) continue;
          if (p.endsWith("package.json")) packageFiles.push(p);
          if (packageFiles.length >= maxPackageFilesPerRoot) break;
        }
      } else {
        packageFiles.push("package.json");
      }
    } else if (hasRg()) {
      const res = spawnSync("rg", ["--files", "--hidden", "-g", "package.json", "-g", "!**/node_modules/**", "-g", "!**/.git/**"], {
        encoding: "utf8",
        cwd: root.abs_path,
      });
      const out = String(res.stdout || "");
      for (const line of out.split("\n")) {
        const p = line.trim();
        if (!p) continue;
        if (!packageFiles.includes(p)) packageFiles.push(p);
        if (packageFiles.length >= maxPackageFilesPerRoot) break;
      }
    } else {
      packageFiles.push("package.json");
    }

    for (const rel of packageFiles.sort((a, b) => a.localeCompare(b))) {
      const pkgText = gitRef
        ? await readTextAtRef({ repoAbs: root.abs_path, gitRef, repoRelPath: rel })
        : await readTextAbs(resolve(root.abs_path, rel));
      if (!pkgText) continue;

      try {
        const pkg = JSON.parse(pkgText);
        const scripts = pkg?.scripts && typeof pkg.scripts === "object" ? pkg.scripts : {};
        const keys = Object.keys(scripts).sort((a, b) => a.localeCompare(b));

        const dirRel = rel.split("/").slice(0, -1).join("/") || ".";
        for (const k of keys) {
          const command = dirRel === "." ? `npm run ${k}` : `npm --prefix ${dirRel} run ${k}`;
          scriptsOut.push({ repo_id: rid, package_json: `${rid}/${rel}`, script: k, command });
        }
      } catch {
        // ignore invalid json
      }
    }
  }

  return scriptsOut;
}
