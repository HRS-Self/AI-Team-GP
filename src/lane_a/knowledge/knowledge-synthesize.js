import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { ensureKnowledgeDirs, loadProjectPaths } from "../../paths/project-paths.js";
import { loadRepoRegistry } from "../../utils/repo-registry.js";
import { ContractValidationError, validateKnowledgeScan } from "../../contracts/validators/index.js";
import { validateKnowledgeGapsFile, validateKnowledgeGap } from "../../validators/knowledge-gap-validator.js";

function nowISO() {
  return new Date().toISOString();
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
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

async function readJsonAbs(absPath) {
  const t = await readFile(resolve(String(absPath || "")), "utf8");
  return JSON.parse(String(t || ""));
}

function listActiveRepoIds(registry) {
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  return repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => normStr(r?.repo_id))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

function parseScanFacts(scanJson) {
  const facts = Array.isArray(scanJson?.facts) ? scanJson.facts : [];
  const claims = facts
    .map((f) => (isPlainObject(f) && typeof f.claim === "string" ? f.claim.trim() : ""))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));

  const entrypoints = [];
  const apiContracts = [];
  const infraFiles = [];

  for (const c of claims) {
    if (c.startsWith("Entrypoint: ")) entrypoints.push(c.slice("Entrypoint: ".length));
    if (c.startsWith("API contract file: ")) apiContracts.push(c.slice("API contract file: ".length));
    if (c.startsWith("Infra file: ")) infraFiles.push(c.slice("Infra file: ".length));
  }

  return {
    entrypoints: Array.from(new Set(entrypoints)).sort((a, b) => a.localeCompare(b)),
    api_contract_files: Array.from(new Set(apiContracts)).sort((a, b) => a.localeCompare(b)),
    infra_files: Array.from(new Set(infraFiles)).sort((a, b) => a.localeCompare(b)),
  };
}

function buildMissingContractGap({ repoId }) {
  const base = {
    scope: "system",
    category: "contract_mismatch",
    severity: "medium",
    risk: "medium",
    summary: `Missing API contract artifact for repo ${repoId}.`,
    expected: "Repository should provide an explicit API contract file (OpenAPI/Swagger, GraphQL schema, or protobuf) in a stable, indexed location.",
    observed: "No API contract file was detected in the repo index/scan evidence bundle.",
    evidence: [
      {
        type: "file",
        path: `evidence/index/repos/${repoId}/repo_index.json`,
        hint: "repo_index.json lists api_surface/fingerprints that were inspected",
      },
      {
        type: "file",
        path: `ssot/repos/${repoId}/scan.json`,
        hint: "scan facts derived from the index did not include any API contract file",
      },
    ],
    suggested_intake: {
      repo_id: repoId,
      title: "Add/Index an API contract artifact",
      body:
        "Lane A evidence scan did not detect an API contract file for this repo (OpenAPI/Swagger, GraphQL schema, or protobuf). Add a contract artifact and ensure it is indexed so cross-repo integration reasoning can be evidence-backed.",
      labels: ["ai", "gap"],
    },
  };
  const v = validateKnowledgeGap(base);
  if (!v.ok) throw new Error(`internal gap build invalid: ${v.errors.join(" | ")}`);
  return v.normalized;
}

export async function runKnowledgeSynthesize({ projectRoot = null, dryRun = false } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, message: reposRes.message };
  const activeRepoIds = listActiveRepoIds(reposRes.registry);
  if (!activeRepoIds.length) return { ok: false, message: "No active repos found in config/REPOS.json." };

  const missing = activeRepoIds.filter((id) => !existsSync(join(paths.knowledge.ssotReposAbs, id, "scan.json")));
  if (missing.length) {
    const msg = `Cannot synthesize: missing scan outputs for ${missing.join(", ")}. Run --knowledge-scan/--knowledge-refresh first.`;
    if (!dryRun) {
      await mkdir(paths.laneA.logsAbs, { recursive: true });
      await writeTextAtomic(join(paths.laneA.logsAbs, "knowledge-synthesize.error.json"), JSON.stringify({ ok: false, captured_at: nowISO(), message: msg, missing }, null, 2) + "\n");
    }
    return { ok: false, message: msg, missing_repo_ids: missing };
  }

  const captured_at = nowISO();
  const inputs = [];
  const integrationRepos = [];

  for (const repoId of activeRepoIds) {
    const scanAbs = join(paths.knowledge.ssotReposAbs, repoId, "scan.json");
    // eslint-disable-next-line no-await-in-loop
    const scan = await readJsonAbs(scanAbs);
    try {
      validateKnowledgeScan(scan);
    } catch (err) {
      const msg = err instanceof ContractValidationError ? err.message : err instanceof Error ? err.message : String(err);
      const out = { ok: false, captured_at: nowISO(), message: "Repo scan contract failed validation.", repo_id: repoId, error: msg, scan_path: scanAbs };
      if (!dryRun) {
        await mkdir(paths.laneA.logsAbs, { recursive: true });
        await writeTextAtomic(join(paths.laneA.logsAbs, "knowledge-synthesize.error.json"), JSON.stringify(out, null, 2) + "\n");
      }
      return { ok: false, message: out.message, repo_id: repoId, error: msg };
    }
    const scanVersion = Number.isFinite(Number(scan?.scan_version)) ? Number(scan.scan_version) : null;
    const scannedAt = typeof scan?.scanned_at === "string" ? scan.scanned_at : null;
    inputs.push({ repo_id: repoId, scanned_at: scannedAt, scan_version: scanVersion });
    integrationRepos.push({ repo_id: repoId, ...parseScanFacts(scan) });
  }

  inputs.sort((a, b) => a.repo_id.localeCompare(b.repo_id));
  integrationRepos.sort((a, b) => a.repo_id.localeCompare(b.repo_id));

  const integration = {
    version: 1,
    scope: "system",
    captured_at,
    inputs,
    integration_map: {
      repos: integrationRepos,
    },
    cross_repo_contracts: [],
    known_unknowns: [],
  };

  const gaps = [];
  for (const r of integrationRepos) {
    if (!Array.isArray(r.api_contract_files) || r.api_contract_files.length === 0) gaps.push(buildMissingContractGap({ repoId: r.repo_id }));
  }

  const gapsFile = validateKnowledgeGapsFile({
    version: 1,
    scope: "system",
    captured_at,
    extractor_version: `knowledge_synthesize/${sha256Hex("v1").slice(0, 8)}`,
    gaps,
  });
  if (!gapsFile.ok) throw new Error(`Internal error: synthesized gaps.json failed validation: ${gapsFile.errors.join(" | ")}`);

  const systemViewsAbs = join(paths.knowledge.viewsAbs, "system");
  const outIntegrationAbs = join(paths.knowledge.ssotSystemAbs, "integration.json");
  const outGapsAbs = join(paths.knowledge.ssotSystemAbs, "gaps.json");
  const outMdAbs = join(systemViewsAbs, "integration.md");

  const mdLines = [];
  mdLines.push("# System Integration");
  mdLines.push("");
  mdLines.push(`captured_at: ${captured_at}`);
  mdLines.push("");
  mdLines.push("## Repos");
  mdLines.push("");
  for (const r of integrationRepos) {
    mdLines.push(`- ${r.repo_id}`);
    if (r.api_contract_files.length) mdLines.push(`  - api_contract_files: ${r.api_contract_files.join(", ")}`);
  }
  mdLines.push("");
  mdLines.push("## Gaps");
  mdLines.push("");
  if (!gapsFile.normalized.gaps.length) mdLines.push("- (none)");
  for (const g of gapsFile.normalized.gaps) mdLines.push(`- ${g.gap_id}: ${g.summary}`);
  mdLines.push("");

  if (!dryRun) {
    await mkdir(dirname(outIntegrationAbs), { recursive: true });
    await mkdir(dirname(outGapsAbs), { recursive: true });
    await mkdir(systemViewsAbs, { recursive: true });
    await writeTextAtomic(outIntegrationAbs, JSON.stringify(integration, null, 2) + "\n");
    await writeTextAtomic(outGapsAbs, JSON.stringify(gapsFile.normalized, null, 2) + "\n");
    await writeTextAtomic(outMdAbs, mdLines.join("\n"));
  }

  const snapshot_hash = sha256Hex(JSON.stringify({ inputs, gaps: gapsFile.normalized.gaps.map((g) => g.gap_id) }));

  return {
    ok: true,
    dry_run: dryRun,
    captured_at,
    snapshot_hash,
    knowledge_repo_dir: paths.knowledge.rootAbs,
    outputs: {
      integration_json: outIntegrationAbs,
      gaps_json: outGapsAbs,
      integration_md: outMdAbs,
    },
    repos: inputs,
    gaps_count: gapsFile.normalized.gaps.length,
  };
}
