import { readFileSync, existsSync } from "node:fs";
import { readFile, writeFile, mkdir, readdir } from "node:fs/promises";
import { resolve, join, isAbsolute, relative, sep, dirname } from "node:path";
import { spawnSync } from "node:child_process";

import { appendFile, ensureDir } from "../utils/fs.js";
import { createLlmClient } from "../llm/client.js";
import { normalizeLlmContentToText } from "../llm/content.js";
import { validateLlmProfiles } from "../validators/llm-profiles-validator.js";
import { readDocsConfigFile, parseDocsConfig } from "./docs-config.js";
import { loadMergedKnowledgeNotes, mergeMergedKnowledge, renderMergedNotesMarkdown } from "./knowledge-merge.js";
import { nowFsSafeUtcTimestamp } from "../utils/naming.js";
import { loadProjectPaths } from "../paths/project-paths.js";
import { evaluateScopeStaleness, writeRefreshRequiredDecisionPacketIfNeeded } from "../lane_a/lane-a-staleness-policy.js";
import { readSufficiencyOrDefault } from "../lane_a/knowledge/knowledge-sufficiency.js";
import { maybePrependSoftStaleBanner, recordSoftStaleObservation } from "../lane_a/staleness/soft-stale-escalation.js";

function nowISO() {
  return new Date().toISOString();
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function isSameOrSubpath(a, b) {
  // true if a == b OR b is inside a
  const rel = relative(a, b);
  if (!rel) return true;
  if (rel.startsWith("..")) return false;
  return !rel.includes(`..${sep}`) && !isAbsolute(rel);
}

function assertSafeExternalPath({ projectRoot, targetAbs, name }) {
  const pr = resolve(String(projectRoot || ""));
  const t = resolve(String(targetAbs || ""));
  if (isSameOrSubpath(pr, t) || isSameOrSubpath(t, pr)) {
    throw new Error(`${name} must not overlap PROJECT_ROOT:\n- PROJECT_ROOT: ${pr}\n- ${name}: ${t}`);
  }
  if (pr === "/opt/GitRepos/AI-Team" || pr.startsWith("/opt/GitRepos/AI-Team/")) {
    throw new Error(`PROJECT_ROOT is invalid (must be a project instance, not the engine repo): ${pr}`);
  }
}

function git(cwdAbs, args, { timeoutMs = 30_000 } = {}) {
  const res = spawnSync("git", ["-C", cwdAbs, ...args], { encoding: "utf8", timeout: timeoutMs });
  return {
    ok: res.status === 0,
    status: res.status,
    stdout: String(res.stdout || ""),
    stderr: String(res.stderr || ""),
  };
}

function isGitWorktree(pathAbs) {
  if (!existsSync(pathAbs)) return false;
  const res = git(pathAbs, ["rev-parse", "--is-inside-work-tree"]);
  const combined = `${res.stdout}\n${res.stderr}`.toLowerCase();
  if (!res.ok && combined.includes("detected dubious ownership")) {
    throw new Error(
      [
        "Git refused to operate due to 'detected dubious ownership' (safe.directory).",
        `Repo: ${pathAbs}`,
        "",
        "Fix (run as the operator account):",
        `  git config --global --add safe.directory ${pathAbs}`,
      ].join("\n"),
    );
  }
  return res.ok && res.stdout.trim() === "true";
}

function readTextAbs(pathAbs) {
  return readFile(pathAbs, "utf8").then((t) => String(t || ""));
}

async function writeTextAbs(pathAbs, text) {
  await mkdir(dirname(pathAbs), { recursive: true });
  await writeFile(pathAbs, text, "utf8");
}

function loadMasterPromptText() {
  const p = resolve("src/writer/templates/sdlc-master-prompt.txt");
  return readFileSync(p, "utf8");
}

function docDefinitions() {
  return [
    { doc_id: "00_Vision", filename: "00_Vision.md", depends_on: [] },
    { doc_id: "01_BRD", filename: "01_BRD.md", depends_on: ["00_Vision"] },
    { doc_id: "02_PRD", filename: "02_PRD.md", depends_on: ["01_BRD"] },
    { doc_id: "03_Architecture", filename: "03_Architecture.md", depends_on: ["02_PRD"] },
    { doc_id: "04_SRS_Part1", filename: "04_SRS_Part1.md", depends_on: ["02_PRD", "03_Architecture"] },
    { doc_id: "05_TestPlan", filename: "05_TestPlan.md", depends_on: ["04_SRS_Part1"] },
    { doc_id: "06_Operations", filename: "06_Operations.md", depends_on: ["03_Architecture"] },
    { doc_id: "07_Security", filename: "07_Security.md", depends_on: ["03_Architecture"] },
    { doc_id: "08_Release", filename: "08_Release.md", depends_on: ["05_TestPlan", "06_Operations"] },
  ];
}

function buildDocIndex(defs) {
  const byId = new Map(defs.map((d) => [d.doc_id, d]));
  return byId;
}

function normalizeDocsArg(docsArg) {
  const raw = String(docsArg || "").trim();
  if (!raw) return { mode: "all", requested: [] };
  if (raw.toLowerCase() === "all") return { mode: "all", requested: [] };
  const parts = raw.split(",").map((x) => x.trim()).filter(Boolean);
  return { mode: "some", requested: parts };
}

function resolveRequestedDocs({ docsArg, defsById }) {
  const parsed = normalizeDocsArg(docsArg);
  if (parsed.mode === "all") return { ok: true, requested_ids: Array.from(defsById.keys()).sort((a, b) => a.localeCompare(b)), requested_raw: "all" };

  const wanted = [];
  for (const r of parsed.requested) {
    const key = String(r || "").trim();
    if (!key) continue;
    const direct = defsById.get(key);
    if (direct) {
      wanted.push(key);
      continue;
    }
    // Case-insensitive match
    const hit = Array.from(defsById.keys()).find((id) => id.toLowerCase() === key.toLowerCase());
    if (hit) wanted.push(hit);
  }
  const uniq = Array.from(new Set(wanted)).sort((a, b) => a.localeCompare(b));
  if (!uniq.length) return { ok: false, message: `Unknown --docs value(s): ${parsed.requested.join(", ")}` };
  return { ok: true, requested_ids: uniq, requested_raw: parsed.requested.join(",") };
}

function closureWithPrereqs({ requestedIds, defsById }) {
  const needed = new Set();
  const visit = (id) => {
    if (needed.has(id)) return;
    needed.add(id);
    const d = defsById.get(id);
    for (const dep of Array.isArray(d?.depends_on) ? d.depends_on : []) visit(dep);
  };
  for (const id of requestedIds) visit(id);
  return Array.from(needed.values()).sort((a, b) => a.localeCompare(b));
}

function topoOrder(defs) {
  // defs already in intended order; enforce deps appear earlier.
  const byId = buildDocIndex(defs);
  const out = [];
  const visiting = new Set();
  const visited = new Set();
  const visit = (id) => {
    if (visited.has(id)) return;
    if (visiting.has(id)) throw new Error(`Doc dependency cycle at ${id}`);
    visiting.add(id);
    const d = byId.get(id);
    for (const dep of Array.isArray(d?.depends_on) ? d.depends_on : []) visit(dep);
    visiting.delete(id);
    visited.add(id);
    out.push(id);
  };
  for (const d of defs) visit(d.doc_id);
  return out;
}

function summarizeRegistry({ reposJson }) {
  const repos = Array.isArray(reposJson?.repos) ? reposJson.repos : [];
  const active = repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => ({
      repo_id: String(r.repo_id || "").trim(),
      name: typeof r.name === "string" ? r.name.trim() : null,
      team_id: typeof r.team_id === "string" ? r.team_id.trim() : null,
      kind: typeof r.Kind === "string" ? r.Kind : typeof r.kind === "string" ? r.kind : null,
      domains: Array.isArray(r.Domains) ? r.Domains : Array.isArray(r.domains) ? r.domains : null,
      usage: typeof r.Usage === "string" ? r.Usage : typeof r.usage === "string" ? r.usage : null,
      is_hexa: r.IsHexa === true || r.is_hexa === true,
    }))
    .sort((a, b) => a.repo_id.localeCompare(b.repo_id));

  return { active_repos: active, active_count: active.length };
}

function summarizeTeams({ teamsJson }) {
  const teams = Array.isArray(teamsJson?.teams) ? teamsJson.teams : [];
  return teams
    .map((t) => ({
      team_id: String(t?.team_id || "").trim(),
      risk_level: typeof t?.risk_level === "string" ? t.risk_level.trim() : null,
      description: typeof t?.description === "string" ? t.description.trim() : null,
    }))
    .filter((t) => t.team_id)
    .sort((a, b) => a.team_id.localeCompare(b.team_id));
}

function pickWriterAgent({ agentsJson }) {
  const agents = Array.isArray(agentsJson?.agents) ? agentsJson.agents : [];
  const enabled = agents.filter((a) => isPlainObject(a) && a.enabled === true);
  const writers = enabled.filter((a) => String(a.role || "").trim() === "writer" && String(a.implementation || "").trim() === "llm");
  const planners = enabled.filter((a) => String(a.role || "").trim() === "planner" && String(a.implementation || "").trim() === "llm");
  const chosen = writers.sort((a, b) => String(a.agent_id).localeCompare(String(b.agent_id)))[0] || planners.sort((a, b) => String(a.agent_id).localeCompare(String(b.agent_id)))[0] || null;
  const llm_profile = chosen && typeof chosen.llm_profile === "string" && chosen.llm_profile.trim() ? chosen.llm_profile.trim() : null;
  return { agent_id: chosen ? String(chosen.agent_id || "").trim() : null, llm_profile };
}

function parseWriterScope(scopeRaw) {
  const raw = String(scopeRaw || "").trim().toLowerCase();
  if (!raw || raw === "all") return { kind: "all", scope: "all", repo_id: null };
  if (raw === "system") return { kind: "system", scope: "system", repo_id: null };
  const m = raw.match(/^repo:([a-z0-9-_]+)$/);
  if (!m) throw new Error("Invalid --scope (expected: system | repo:<repo_id> | all).");
  return { kind: "repo", scope: raw, repo_id: m[1] };
}

export async function runWriter({
  projectRoot,
  scope = "all",
  docs = "all",
  limit = null,
  dryRun = false,
  forceStaleOverride = false,
  by = null,
  reason = null,
}) {
  const runId = nowFsSafeUtcTimestamp();

  const logLine = async (line) => {
    await ensureDir("ai/lane_a/logs");
    await appendFile("ai/lane_a/logs/writer.log", `${nowISO()} ${line}\n`);
  };

  try {
    if (!projectRoot || !isAbsolute(String(projectRoot))) {
      return { ok: false, message: "--projectRoot must be an absolute path." };
    }

    const docsCfgText = await readDocsConfigFile(projectRoot);
    if (!docsCfgText.ok) return { ok: false, message: docsCfgText.message };
    const docsCfgParsed = parseDocsConfig({ text: docsCfgText.text });
    if (!docsCfgParsed.ok) return { ok: false, message: docsCfgParsed.message };
    const docsCfg = docsCfgParsed.normalized;

    // Paths must be external (never inside runtime project state).
    assertSafeExternalPath({ projectRoot, targetAbs: docsCfg.docs_repo_path, name: "docs_repo_path" });
    assertSafeExternalPath({ projectRoot, targetAbs: docsCfg.knowledge_repo_path, name: "knowledge_repo_path" });

    const paths = await loadProjectPaths({ projectRoot });
    // Writer must read (but never mutate) Lane A SUFFICIENCY.json for gating and UI consistency.
    await readSufficiencyOrDefault({ projectRoot: paths.opsRootAbs });
    if (resolve(docsCfg.knowledge_repo_path) !== paths.knowledge.rootAbs) {
      return { ok: false, message: `config/DOCS.json.knowledge_repo_path must equal K_ROOT.\nExpected: ${paths.knowledge.rootAbs}\nGot: ${docsCfg.knowledge_repo_path}` };
    }
    if (resolve(docsCfg.docs_repo_path) !== paths.knowledge.rootAbs) {
      return { ok: false, message: `config/DOCS.json.docs_repo_path must equal K_ROOT (writer outputs only to knowledge/docs).\nExpected: ${paths.knowledge.rootAbs}\nGot: ${docsCfg.docs_repo_path}` };
    }

    await appendFile(
      "ai/lane_a/ledger.jsonl",
      JSON.stringify({
        timestamp: nowISO(),
        action: "writer_started",
        project_key: docsCfg.project_key,
        scope,
        docs,
        dry_run: dryRun,
        run_id: runId,
      }) + "\n",
    );

    await logLine(`writer_started project=${docsCfg.project_key} scope=${scope} docs=${docs} dry_run=${dryRun}`);

    // Require git worktrees at both paths.
    if (!isGitWorktree(docsCfg.docs_repo_path)) {
      const message = `Docs repo path is missing or not a git worktree: ${docsCfg.docs_repo_path}\nCreate/clone it and run again.`;
      await appendFile("ai/lane_a/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "writer_failed", project_key: docsCfg.project_key, scope, reason_code: "docs_repo_missing", error: message, run_id: runId }) + "\n");
      return { ok: false, message };
    }
    if (!isGitWorktree(docsCfg.knowledge_repo_path)) {
      const message = `Knowledge repo path is missing or not a git worktree: ${docsCfg.knowledge_repo_path}\nCreate/clone it and run again.`;
      await appendFile("ai/lane_a/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "writer_failed", project_key: docsCfg.project_key, scope, reason_code: "knowledge_repo_missing", error: message, run_id: runId }) + "\n");
      return { ok: false, message };
    }

    // Load truth files (project-scoped).
    const reposText = await readTextAbs(resolve(projectRoot, "config", "REPOS.json"));
    const teamsText = await readTextAbs(resolve(projectRoot, "config", "TEAMS.json"));
    const policiesText = await readTextAbs(resolve(projectRoot, "config", "POLICIES.json"));
    const llmProfilesText = await readTextAbs(resolve(projectRoot, "config", "LLM_PROFILES.json"));
    const agentsText = existsSync(resolve(projectRoot, "config", "AGENTS.json")) ? await readTextAbs(resolve(projectRoot, "config", "AGENTS.json")) : null;

    const reposJson = JSON.parse(reposText);
    const teamsJson = JSON.parse(teamsText);
    const policiesJson = JSON.parse(policiesText);
    const llmProfilesJson = JSON.parse(llmProfilesText);
    const agentsJson = agentsText ? JSON.parse(agentsText) : null;

    const repoSummary = summarizeRegistry({ reposJson });
    const teamsSummary = summarizeTeams({ teamsJson });
    const agent = pickWriterAgent({ agentsJson });

    const scopeInfo = parseWriterScope(scope);
    const scopeKey = scopeInfo.kind === "repo" ? `repo:${scopeInfo.repo_id}` : "system";
    const st = await evaluateScopeStaleness({ paths, registry: { ...reposJson, base_dir: paths.reposRootAbs }, scope: scopeKey });
    const stalenessSnapshot = {
      scope: scopeKey,
      stale: st.stale === true,
      hard_stale: st.hard_stale === true,
      reasons: Array.isArray(st.reasons) ? st.reasons.slice().sort((a, b) => a.localeCompare(b)) : [],
      stale_repos: Array.isArray(st.stale_repos) ? st.stale_repos.slice().sort((a, b) => a.localeCompare(b)) : [],
      hard_stale_repos: Array.isArray(st.hard_stale_repos) ? st.hard_stale_repos.slice().sort((a, b) => a.localeCompare(b)) : [],
    };
    await recordSoftStaleObservation({
      paths,
      scope: scopeKey,
      stalenessSnapshot,
    });
    const staleMarker = st.stale === true;
    const staleRepoIds = Array.isArray(st.stale_repos) ? st.stale_repos.slice().sort((a, b) => a.localeCompare(b)) : [];
    if (st.hard_stale && !forceStaleOverride) {
      const decisions_written = [];
      if (scopeInfo.kind === "repo") {
        const d = await writeRefreshRequiredDecisionPacketIfNeeded({
          paths,
          repoId: scopeInfo.repo_id,
          blockingState: "WRITER",
          staleInfo: { stale_reason: st.reasons[0] || "stale", stale_reasons: st.reasons },
          producer: "writer",
          dryRun,
        });
        if (d?.json_abs) decisions_written.push(d.json_abs);
      } else {
        const d = await writeRefreshRequiredDecisionPacketIfNeeded({
          paths,
          repoId: null,
          blockingState: "WRITER",
          staleInfo: { stale_reason: st.reasons[0] || "stale", stale_reasons: st.reasons, stale_repos: st.stale_repos || [] },
          producer: "writer",
          dryRun,
        });
        if (d?.json_abs) decisions_written.push(d.json_abs);
      }
      await appendFile(
        "ai/lane_a/ledger.jsonl",
        JSON.stringify({ timestamp: nowISO(), action: "writer_failed", project_key: docsCfg.project_key, scope, reason_code: "knowledge_stale", reasons: st.reasons, run_id: runId }) + "\n",
      );
      return { ok: false, reason_code: "STALE_BLOCKED", error: "knowledge_stale", scope: scopeInfo.scope, reasons: st.reasons, decisions_written };
    }

    if (st.stale && forceStaleOverride) {
      const who = typeof by === "string" && by.trim() ? by.trim() : (process.env.USER || "").trim() || null;
      const why = typeof reason === "string" && reason.trim() ? reason.trim() : null;
      await appendFile(
        "ai/lane_a/ledger.jsonl",
        JSON.stringify({ timestamp: nowISO(), type: "stale_override", command: "writer", scope: scopeInfo.scope, by: who, reason: why, run_id: runId }) + "\n",
      );
    }

    if (!agentsJson || agentsJson.version !== 3 || !Array.isArray(agentsJson.agents)) {
      return { ok: false, message: "Invalid config/AGENTS.json (expected {version:3, agents:[...]}). Run: node src/cli.js --agents-migrate" };
    }
    if (agentsJson.agents.some((a) => a && typeof a === "object" && Object.prototype.hasOwnProperty.call(a, "model"))) {
      return { ok: false, message: "AGENTS.json contains legacy key 'model'. Run: node src/cli.js --agents-migrate" };
    }
    const profV = validateLlmProfiles(llmProfilesJson);
    if (!profV.ok) return { ok: false, message: `Invalid config/LLM_PROFILES.json: ${profV.errors.join(" | ")}` };
    const profiles = profV.normalized.profiles;
    if (!agent.agent_id) return { ok: false, message: "No enabled LLM writer/planner agent found (role=writer or planner)." };
    if (!agent.llm_profile) return { ok: false, message: `Agent ${agent.agent_id} missing llm_profile (implementation=llm).` };
    if (!Object.prototype.hasOwnProperty.call(profiles, agent.llm_profile)) return { ok: false, message: `Agent ${agent.agent_id} references unknown llm_profile '${agent.llm_profile}'.` };
    const agentProfile = profiles[agent.llm_profile];
    const provider = typeof agentProfile.provider === "string" ? agentProfile.provider.trim() : "";
    const model = typeof agentProfile.model === "string" ? agentProfile.model.trim() : "";
    if (!provider || !model) return { ok: false, message: `Invalid llm_profile '${agent.llm_profile}': provider/model must be non-empty.` };

    // Build a deterministic knowledge pack. Writer reads ONLY from K_ROOT.
    const readJsonIfExists = async (absPath) => {
      const abs = resolve(absPath);
      if (!existsSync(abs)) return { ok: true, exists: false, json: null, path: abs };
      const text = await readTextAbs(abs);
      return { ok: true, exists: true, json: JSON.parse(text), path: abs };
    };

    const assumptionsRes = await loadMergedKnowledgeNotes({ knowledgeRepoPath: paths.knowledge.rootAbs, relPath: "ssot/system/assumptions.json" });
    const baseMerged =
      assumptionsRes.ok
        ? assumptionsRes.merged
        : {
            version: 1,
            scope: "system",
            updated_at: nowISO(),
            sources: [],
            invariants: [],
            boundaries: [],
            constraints: [],
            risks: [],
            open_questions: [],
            decisions_needed: [],
          };

    const mergedRendered = renderMergedNotesMarkdown({
      projectKey: docsCfg.project_key,
      scope: scopeInfo.scope,
      merged: baseMerged,
      sources: assumptionsRes.ok ? [assumptionsRes.path] : [],
    });

    const extra = [];
    const addJsonSection = (title, json, srcPath) => {
      if (!json) return;
      extra.push(`## ${title}`);
      if (srcPath) extra.push(`- source: ${srcPath}`);
      extra.push("");
      extra.push("```json");
      extra.push(JSON.stringify(json, null, 2));
      extra.push("```");
      extra.push("");
    };

    if (scopeInfo.kind === "system" || scopeInfo.kind === "all") {
      const integration = await readJsonIfExists(resolve(paths.knowledge.rootAbs, "ssot/system/integration.json"));
      if (integration.exists) addJsonSection("System Integration (SSOT)", integration.json, integration.path);
    }

    if (scopeInfo.kind === "repo") {
      const scan = await readJsonIfExists(resolve(paths.knowledge.rootAbs, "ssot/repos", scopeInfo.repo_id, "scan.json"));
      if (scan.exists) addJsonSection(`Repo Scan (SSOT) — ${scopeInfo.repo_id}`, scan.json, scan.path);

      const arch = await readJsonIfExists(resolve(paths.knowledge.rootAbs, "ssot/repos", scopeInfo.repo_id, "committee", "architect_claims.json"));
      if (arch.exists) addJsonSection(`Committee Architect Claims — ${scopeInfo.repo_id}`, arch.json, arch.path);
      const skeptic = await readJsonIfExists(resolve(paths.knowledge.rootAbs, "ssot/repos", scopeInfo.repo_id, "committee", "skeptic_challenges.json"));
      if (skeptic.exists) addJsonSection(`Committee Skeptic Challenges — ${scopeInfo.repo_id}`, skeptic.json, skeptic.path);
      const status = await readJsonIfExists(resolve(paths.knowledge.rootAbs, "ssot/repos", scopeInfo.repo_id, "committee", "committee_status.json"));
      if (status.exists) addJsonSection(`Committee Status — ${scopeInfo.repo_id}`, status.json, status.path);
    }

    if (scopeInfo.kind === "all") {
      const reposDirAbs = resolve(paths.knowledge.rootAbs, "ssot", "repos");
      if (existsSync(reposDirAbs)) {
        const entries = await readdir(reposDirAbs, { withFileTypes: true });
        const repoIds = entries.filter((e) => e.isDirectory()).map((e) => e.name).sort((a, b) => a.localeCompare(b));
        const maxRepos = 50;
        const meta = [];
        for (const repoId of repoIds.slice(0, maxRepos)) {
          // eslint-disable-next-line no-await-in-loop
          const s = await readJsonIfExists(resolve(reposDirAbs, repoId, "scan.json"));
          const scannedAt = s.exists && typeof s.json?.scanned_at === "string" ? s.json.scanned_at : null;
          meta.push({ repo_id: repoId, scanned_at: scannedAt, scan_path: s.exists ? relative(paths.knowledge.rootAbs, s.path) : null });
        }
        addJsonSection("Repo Scan Index (SSOT)", { version: 1, scope: "system", repos: meta }, null);
      }
    }

    const knowledgePackMarkdownBase = [mergedRendered.markdown, ...extra].filter(Boolean).join("\n") + "\n";
    const knowledgePackMarkdown = maybePrependSoftStaleBanner({
      markdown: knowledgePackMarkdownBase,
      stalenessSnapshot,
    });

    const artifactRoot = resolve(paths.laneA.logsAbs, "writer_artifacts", dryRun ? "dry-run" : "run", runId);
    await mkdir(artifactRoot, { recursive: true });
    await writeTextAbs(resolve(artifactRoot, "KNOWLEDGE_PACK.md"), knowledgePackMarkdown);
    const writerStatusJsonAbs = resolve(artifactRoot, "WRITER_STATUS.json");
    const writerStatusMdAbs = resolve(artifactRoot, "STATUS.md");

    const writeWriterStatusArtifacts = async ({ generatedDocs = [], message = null } = {}) => {
      const generated = Array.isArray(generatedDocs)
        ? generatedDocs.map((g) => ({
            doc_id: String(g?.doc_id || "").trim(),
            path: String(g?.path || "").trim(),
          }))
        : [];
      const softStale = stalenessSnapshot.stale === true && stalenessSnapshot.hard_stale !== true;
      const status = {
        version: 1,
        run_id: runId,
        scope: scopeInfo.scope,
        stale: stalenessSnapshot.stale === true,
        hard_stale: stalenessSnapshot.hard_stale === true,
        staleness: stalenessSnapshot,
        degraded: softStale,
        generated_docs: generated,
        message: typeof message === "string" && message.trim() ? message.trim() : null,
      };
      if (softStale) status.degraded_reason = "soft_stale";
      await writeTextAbs(writerStatusJsonAbs, JSON.stringify(status, null, 2) + "\n");

      const lines = [];
      lines.push("# Writer Status");
      lines.push("");
      lines.push(`- run_id: ${runId}`);
      lines.push(`- scope: ${scopeInfo.scope}`);
      lines.push(`- stale: ${status.stale ? "true" : "false"}`);
      lines.push(`- hard_stale: ${status.hard_stale ? "true" : "false"}`);
      lines.push(`- degraded: ${status.degraded ? "true" : "false"}`);
      if (status.degraded_reason) lines.push(`- degraded_reason: ${status.degraded_reason}`);
      if (status.message) lines.push(`- message: ${status.message}`);
      lines.push("");
      lines.push("## Generated Docs");
      lines.push("");
      if (!generated.length) lines.push("- (none)");
      for (const g of generated) lines.push(`- ${g.doc_id}: ${g.path}`);
      lines.push("");

      const statusMd = maybePrependSoftStaleBanner({
        markdown: lines.join("\n") + "\n",
        stalenessSnapshot,
      });
      await writeTextAbs(writerStatusMdAbs, statusMd);
    };

    const defs = docDefinitions();
    const byId = buildDocIndex(defs);
    const ordered = topoOrder(defs);

    const req = resolveRequestedDocs({ docsArg: docs, defsById: byId });
    if (!req.ok) return { ok: false, message: req.message };
    const closure = closureWithPrereqs({ requestedIds: req.requested_ids, defsById: byId });
    const wantedSet = new Set(closure);
    const requestedSet = new Set(req.requested_ids);

    const maxPerRun = Math.max(1, Number.isFinite(Number(limit)) ? Number(limit) : docsCfg.max_docs_per_run);
    const docsToGenerate = [];

    for (const id of ordered) {
      if (!wantedSet.has(id)) continue;
      const def = byId.get(id);
      const targetAbs = resolve(docsCfg.docs_repo_path, def.filename);
      const exists = existsSync(targetAbs);
      const shouldGenerate = requestedSet.has(id) || !exists; // prerequisites only if missing
      if (!shouldGenerate) continue;
      docsToGenerate.push(id);
      if (docsToGenerate.length >= maxPerRun) break;
    }

    if (!docsToGenerate.length) {
      await writeWriterStatusArtifacts({ generatedDocs: [], message: "No docs needed generation." });
      await logLine("writer_noop (all requested docs already exist and no prerequisites missing)");
      return {
        ok: true,
        dry_run: dryRun,
        run_id: runId,
        generated: [],
        message: "No docs needed generation.",
        artifacts_dir: relative(projectRoot, artifactRoot),
        writer_status_json: relative(projectRoot, writerStatusJsonAbs),
        writer_status_md: relative(projectRoot, writerStatusMdAbs),
        stale: staleMarker,
        stale_repos: staleRepoIds,
      };
    }

    // Docs repo must be clean before we start if commit is enabled and allow_dirty=false.
    if (!dryRun && docsCfg.commit.enabled && docsCfg.commit.allow_dirty !== true) {
      const status = git(docsCfg.docs_repo_path, ["status", "--porcelain"]);
      if (!status.ok) return { ok: false, message: `Failed to read docs repo status: ${status.stderr.trim() || status.stdout.trim()}` };
      if (status.stdout.trim()) {
        return { ok: false, message: `Docs repo is dirty; refuse to proceed. Clean it first or set commit.allow_dirty=true in config/DOCS.json.\n${status.stdout.trim()}` };
      }
    }

    const sys = loadMasterPromptText();
    const timeoutRaw = process.env.WRITER_TIMEOUT_MS;
    const timeoutMs = Number.isFinite(Number(timeoutRaw)) && Number(timeoutRaw) > 0 ? Number(timeoutRaw) : 60_000;

    const { ok: llmOk, llm, model: resolvedModel, message: llmMessage } = createLlmClient({ provider, model, timeoutMs });
    if (!llmOk) return { ok: false, message: `Writer LLM unavailable: ${llmMessage}` };

    const generated = [];
    const writtenFiles = [];

    for (const docId of docsToGenerate) {
      const def = byId.get(docId);
      const deps = Array.isArray(def.depends_on) ? def.depends_on.slice() : [];

      const depsContent = [];
      for (const depId of deps) {
        const dep = byId.get(depId);
        const depPath = resolve(docsCfg.docs_repo_path, dep.filename);
        if (!existsSync(depPath)) continue;
        const text = await readTextAbs(depPath);
        depsContent.push({ doc_id: depId, filename: dep.filename, content: text.slice(0, 20000) });
      }

      const payload = {
        project_key: docsCfg.project_key,
        doc_id: def.doc_id,
        filename: def.filename,
        depends_on: deps,
        parts_word_target: docsCfg.parts_word_target,
        output_format: docsCfg.output_format,
        inputs: {
          repos_summary: repoSummary,
          teams_summary: teamsSummary,
          policies: policiesJson,
          merged_notes_md: knowledgePackMarkdown,
          open_questions: mergedRendered.open_questions,
          decisions_needed: mergedRendered.decisions_needed,
          dependency_docs: depsContent,
          project_truth_paths: {
            repos: "config/REPOS.json",
            teams: "config/TEAMS.json",
            policies: "config/POLICIES.json",
            knowledge_ssot_assumptions: "ssot/system/assumptions.json",
          },
        },
        constraints: {
          must_not_fabricate: true,
          must_include_open_questions_when_missing_info: true,
        },
      };

      const messages = [
        { role: "system", content: sys },
        { role: "user", content: JSON.stringify(payload, null, 2) },
      ];

      let out;
      try {
        const res = await llm.invoke(messages);
        const norm = normalizeLlmContentToText(res?.content);
        out = norm.text;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        await appendFile(
          "ai/lane_a/ledger.jsonl",
          JSON.stringify({ timestamp: nowISO(), action: "writer_failed", project_key: docsCfg.project_key, scope, doc_id: docId, reason_code: "llm_error", error: msg, model }) + "\n",
        );
        return { ok: false, message: `Writer failed (LLM error) for ${docId}: ${msg}` };
      }

      const targetDir = dryRun ? artifactRoot : docsCfg.docs_repo_path;
      const targetAbs = resolve(targetDir, def.filename);
      await writeTextAbs(targetAbs, String(out || "").trimEnd() + "\n");

      generated.push({ doc_id: docId, path: dryRun ? relative(projectRoot, targetAbs) : targetAbs });
      writtenFiles.push(targetAbs);

      await appendFile(
        "ai/lane_a/ledger.jsonl",
        JSON.stringify({ timestamp: nowISO(), action: "writer_doc_generated", project_key: docsCfg.project_key, scope, doc_id: docId, output: dryRun ? relative(projectRoot, targetAbs) : targetAbs, dry_run: dryRun, model }) + "\n",
      );
      await logLine(`writer_doc_generated doc_id=${docId} path=${targetAbs}`);
    }

    // Write INDEX.md
    {
      const indexTargetDir = dryRun ? artifactRoot : docsCfg.docs_repo_path;
      const indexAbs = resolve(indexTargetDir, "INDEX.md");
      const now = nowISO();
      const table = [];
      table.push("# Docs Index");
      table.push("");
      table.push(`- project_key: ${docsCfg.project_key}`);
      table.push(`- scope: ${scope}`);
      table.push(`- last_generated_at: ${now}`);
      table.push("");
      table.push("| doc_id | filename | status | last_generated_at |");
      table.push("|---|---|---|---|");
      for (const d of defs) {
        const generatedNow = generated.find((g) => g.doc_id === d.doc_id);
        const filePath = resolve(indexTargetDir, d.filename);
        const exists = existsSync(filePath);
        const status = generatedNow ? "generated" : exists ? "present" : "missing";
        const ts = generatedNow ? now : "";
        table.push(`| ${d.doc_id} | ${d.filename} | ${status} | ${ts} |`);
      }
      table.push("");
      await writeTextAbs(indexAbs, table.join("\n"));
      writtenFiles.push(indexAbs);
    }

    // Commit & push
    if (!dryRun && docsCfg.commit.enabled) {
      const branch = docsCfg.commit.branch;
      const sw = git(docsCfg.docs_repo_path, ["switch", branch], { timeoutMs: 60_000 });
      if (!sw.ok) {
        return { ok: false, message: `Failed to switch docs repo to branch ${branch}. Create it and try again.\n${(sw.stderr || sw.stdout).trim()}` };
      }

      const relFiles = writtenFiles
        .map((p) => relative(docsCfg.docs_repo_path, p))
        .filter((p) => p && !p.startsWith(".."));
      const add = git(docsCfg.docs_repo_path, ["add", ...relFiles], { timeoutMs: 60_000 });
      if (!add.ok) return { ok: false, message: `Failed to git add docs outputs.\n${(add.stderr || add.stdout).trim()}` };

      const msg = generated.length === 1 ? `docs: generate ${generated[0].doc_id} (project=${docsCfg.project_key})` : `docs: generate ${generated[0].doc_id} (+${generated.length - 1}) (project=${docsCfg.project_key})`;
      const commitRes = git(docsCfg.docs_repo_path, ["commit", "-m", msg], { timeoutMs: 60_000 });
      if (!commitRes.ok && !String(commitRes.stderr || "").toLowerCase().includes("nothing to commit")) {
        return { ok: false, message: `Failed to git commit docs outputs.\n${(commitRes.stderr || commitRes.stdout).trim()}` };
      }

      const sha = git(docsCfg.docs_repo_path, ["rev-parse", "HEAD"]);
      const commit = sha.ok ? sha.stdout.trim() : null;

      const push = git(docsCfg.docs_repo_path, ["push", "origin", branch], { timeoutMs: 120_000 });
      if (!push.ok) {
        await appendFile(
          "ai/lane_a/ledger.jsonl",
          JSON.stringify({ timestamp: nowISO(), action: "writer_failed", project_key: docsCfg.project_key, scope, reason_code: "push_failed", branch, commit, error: (push.stderr || push.stdout).trim() }) + "\n",
        );
        return { ok: false, message: `Docs push failed.\n${(push.stderr || push.stdout).trim()}`, commit, branch };
      }

      await appendFile(
        "ai/lane_a/ledger.jsonl",
        JSON.stringify({ timestamp: nowISO(), action: "writer_commit_pushed", project_key: docsCfg.project_key, scope, branch, commit }) + "\n",
      );
      await logLine(`writer_commit_pushed branch=${branch} commit=${commit || "unknown"}`);
    }

    await writeWriterStatusArtifacts({ generatedDocs: generated, message: null });

    return {
      ok: true,
      dry_run: dryRun,
      run_id: runId,
      project_key: docsCfg.project_key,
      scope,
      generated,
      artifacts_dir: relative(projectRoot, artifactRoot),
      writer_status_json: relative(projectRoot, writerStatusJsonAbs),
      writer_status_md: relative(projectRoot, writerStatusMdAbs),
      docs_repo_path: docsCfg.docs_repo_path,
      knowledge_repo_path: docsCfg.knowledge_repo_path,
      stale: staleMarker,
      stale_repos: staleRepoIds,
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    await appendFile("ai/lane_a/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "writer_failed", reason_code: "exception", error: msg, run_id: runId }) + "\n");
    await logLine(`writer_failed ${msg}`);
    return { ok: false, message: msg };
  }
}
