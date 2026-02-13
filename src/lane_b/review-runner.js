import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { createHash } from "node:crypto";
import { readdir } from "node:fs/promises";
import { appendFile, ensureDir, readTextIfExists, writeText } from "../utils/fs.js";
import { nowTs } from "../utils/id.js";
import { getRepoPathsForWork } from "../utils/repo-registry.js";
import { hasRg, scanWithRgInRoots, scanFallbackInRoots } from "../utils/repo-scan.js";
import { resolveStatePath, getAIProjectRoot } from "../project/state-paths.js";
import { resolveSsotBundle } from "../ssot/ssot-resolver.js";
import { ensureWorkSsotBundle, renderSsotExcerptsForLlm } from "../ssot/work-ssot-bundle.js";
import { jsonStableStringify } from "../utils/json.js";

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function parseTeamsCsv(csv) {
  if (!csv) return null;
  const out = [];
  for (const part of String(csv).split(",")) {
    const t = part.trim();
    if (!t) continue;
    if (!out.includes(t)) out.push(t);
  }
  return out.length ? out : null;
}

function flagsReviewerOn(flagsText) {
  return String(flagsText || "")
    .split("\n")
    .some((l) => l.trim().toLowerCase() === "reviewer: on");
}

function validateReviewJson(obj) {
  const errors = [];
  const allowedRisk = new Set(["Low", "Med", "High"]);
  const allowedNext = new Set(["Revise proposals", "Proceed to patch plan", "Escalate"]);

  if (!Array.isArray(obj?.ssot_references)) {
    errors.push("ssot_references must be an array.");
  } else {
    for (let i = 0; i < obj.ssot_references.length; i += 1) {
      const r = obj.ssot_references[i];
      const doc = typeof r?.doc === "string" ? r.doc.trim() : "";
      const section = typeof r?.section === "string" ? r.section.trim() : "";
      const rule_id = typeof r?.rule_id === "string" ? r.rule_id.trim() : "";
      if (!doc || !section || !rule_id) errors.push(`ssot_references[${i}] must include non-empty {doc, section, rule_id}.`);
    }
    if (obj.ssot_references.length < 1) errors.push("ssot_references must contain at least 1 entry.");
  }

  if (!allowedRisk.has(obj?.summary_verdict_risk)) errors.push("summary_verdict_risk must be Low|Med|High.");
  if (!Array.isArray(obj?.what_looks_solid) || !obj.what_looks_solid.every((x) => typeof x === "string")) errors.push("what_looks_solid must be string[].");
  if (!Array.isArray(obj?.what_is_missing_or_unclear) || !obj.what_is_missing_or_unclear.every((x) => typeof x === "string"))
    errors.push("what_is_missing_or_unclear must be string[].");
  if (!Array.isArray(obj?.risks_not_addressed) || !obj.risks_not_addressed.every((x) => typeof x === "string")) errors.push("risks_not_addressed must be string[].");
  if (!Array.isArray(obj?.cross_team_inconsistencies) || !obj.cross_team_inconsistencies.every((x) => typeof x === "string"))
    errors.push("cross_team_inconsistencies must be string[].");
  if (
    !Array.isArray(obj?.questions_to_answer_before_approval) ||
    !obj.questions_to_answer_before_approval.every((x) => typeof x === "string")
  )
    errors.push("questions_to_answer_before_approval must be string[].");
  if (!allowedNext.has(obj?.suggested_next_step)) errors.push("suggested_next_step must be one of: Revise proposals | Proceed to patch plan | Escalate.");

  return { ok: errors.length === 0, errors };
}

function renderReviewMarkdown({ workId, timestamp, teamsReviewed, missingProposals, repoRootsScanned, review }) {
  const bullets = (arr) => {
    if (!Array.isArray(arr) || arr.length === 0) return ["- (none)"];
    return arr.map((x) => `- ${String(x)}`);
  };

  return [
    "# Architect review",
    "",
    `Work item: ${workId}`,
    `Timestamp: ${timestamp}`,
    "",
    "## Inputs reviewed",
    "",
    `- Teams reviewed: ${teamsReviewed.length ? teamsReviewed.join(", ") : "(none)"}`,
    `- Teams missing proposals: ${missingProposals.length ? missingProposals.join(", ") : "(none)"}`,
    `- Repo roots scanned: ${
      repoRootsScanned && repoRootsScanned.length
        ? repoRootsScanned.map((r) => `${String(r.repo_id)}(${r.exists ? "exists" : "missing"})`).join(", ")
        : "(none)"
    }`,
    "",
    "## Summary verdict (Low/Med/High risk)",
    "",
    `- ${review.summary_verdict_risk}`,
    "",
    "## What looks solid",
    "",
    ...bullets(review.what_looks_solid),
    "",
    "## What is missing / unclear",
    "",
    ...bullets(review.what_is_missing_or_unclear),
    "",
    "## Risks not addressed",
    "",
    ...bullets(review.risks_not_addressed),
    "",
    "## Cross-team inconsistencies",
    "",
    ...bullets(review.cross_team_inconsistencies),
    "",
    "## Questions to answer before approval",
    "",
    ...bullets(review.questions_to_answer_before_approval),
    "",
    "## Suggested next step",
    "",
    `- ${review.suggested_next_step}`,
    "",
  ].join("\n");
}

function tokenizeTerms(text) {
  const stop = new Set([
    "the",
    "and",
    "or",
    "to",
    "for",
    "of",
    "in",
    "on",
    "a",
    "an",
    "is",
    "are",
    "be",
    "with",
    "by",
    "from",
    "this",
    "that",
    "it",
    "we",
    "you",
    "work",
    "item",
    "team",
    "scope",
    "tasks",
    "task",
    "notes",
    "open",
    "questions",
    "review",
    "draft",
    "identify",
    "confirm",
  ]);

  const cleaned = String(text || "")
    .toLowerCase()
    .replace(/[`"'.,:;()[\]{}<>!?/\\|+=_*~-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const tokens = cleaned.split(" ").filter(Boolean);
  const counts = new Map();
  for (const t of tokens) {
    if (stop.has(t)) continue;
    if (t.length < 4 && !["ui", "api", "jwt", "oidc", "oauth", "rn"].includes(t)) continue;
    counts.set(t, (counts.get(t) || 0) + 1);
  }
  return counts;
}

function deriveSearchTerms({ intakeMd, tasksByTeam, proposalsByTeam }) {
  const taskText = Object.values(tasksByTeam || {})
    .filter(Boolean)
    .join("\n");
  const proposalText = Object.values(proposalsByTeam || {})
    .flat()
    .map((p) => p?.text || "")
    .join("\n");

  const counts = tokenizeTerms(`${intakeMd}\n${taskText}\n${proposalText}`);
  const base = Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([t]) => t)
    .slice(0, 12);

  const seeds = ["portal", "dashboard", "copy", "mobile", "auth", "token", "api", "endpoint"];
  const out = [];
  for (const t of [...seeds, ...base]) {
    const v = String(t || "").trim().toLowerCase();
    if (!v) continue;
    if (!out.includes(v)) out.push(v);
    if (out.length >= 14) break;
  }
  return out;
}

async function updatePlanWithOptionalReview(planText) {
  const line = "- Optional: run --review (disabled by default)";
  const lines = String(planText || "").split("\n");
  const nextIdx = lines.findIndex((l) => l.trim() === "## Next");
  if (nextIdx === -1) return planText;

  let end = lines.length;
  for (let i = nextIdx + 1; i < lines.length; i += 1) {
    if (lines[i].startsWith("## ") && lines[i].trim() !== "## Next") {
      end = i;
      break;
    }
  }
  const has = lines.slice(nextIdx, end).some((l) => l.trim() === line);
  if (has) return planText;

  return [...lines.slice(0, end), line, ...lines.slice(end)].join("\n");
}

async function generateReviewWithRetries({ llm, systemPrompt, userPrompt }) {
  const attempts = [];

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const response = await llm.invoke([
      { role: "system", content: systemPrompt },
      { role: "user", content: userPrompt },
    ]);

    const raw = typeof response?.content === "string" ? response.content : String(response?.content ?? "");
    let parsed = null;
    let parseError = null;

    try {
      parsed = JSON.parse(raw);
    } catch (err) {
      parseError = err instanceof Error ? err.message : String(err);
    }

    if (parsed) {
      const v = validateReviewJson(parsed);
      if (v.ok) {
        return { ok: true, review: parsed, rawModelOutput: raw, attempts: attempt };
      }
      attempts.push({ attempt, raw, error: `Schema validation failed: ${v.errors.join(" ")}` });
    } else {
      attempts.push({ attempt, raw, error: `JSON parse failed: ${parseError}` });
    }
  }

  return { ok: false, attempts };
}

export async function runReview({ repoRoot, workId, teamsCsv }) {
  const enabled = String(process.env.REVIEWER_ENABLED || "false").trim().toLowerCase() === "true";
  if (!enabled) return { ok: false, message: "Reviewer disabled (set REVIEWER_ENABLED=true)" };

  const flagsPath = `ai/lane_b/work/${workId}/FLAGS.md`;
  const flagsText = await readTextIfExists(flagsPath);
  if (!flagsText || !flagsReviewerOn(flagsText)) {
    return { ok: false, message: "Reviewer not enabled for this work item (set Reviewer: ON in FLAGS.md)" };
  }

  let projectRoot;
  try {
    projectRoot = getAIProjectRoot({ required: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Cannot review: ${msg}` };
  }

  const workDir = `ai/lane_b/work/${workId}`;
  const intakeMd = await readTextIfExists(`${workDir}/INTAKE.md`);
  const routingJson = await readTextIfExists(`${workDir}/ROUTING.json`);
  if (!intakeMd) return { ok: false, message: `Missing ${workDir}/INTAKE.md` };
  if (!routingJson) return { ok: false, message: `Missing ${workDir}/ROUTING.json` };

  let routing;
  try {
    routing = JSON.parse(routingJson);
  } catch {
    return { ok: false, message: `Invalid JSON in ${workDir}/ROUTING.json` };
  }

  const requestedTeams = parseTeamsCsv(teamsCsv);
  const selectedTeams = Array.isArray(routing?.selected_teams) ? routing.selected_teams.slice().filter(Boolean) : [];
  const teams = (requestedTeams || selectedTeams).filter(Boolean);
  if (!teams.length) return { ok: false, message: "No teams selected for review." };

  const intakeTaggedMd = await readTextIfExists(`${workDir}/INTAKE_TAGGED.md`);

  const tasksByTeam = {};
  for (const t of teams) {
    const task = await readTextIfExists(`${workDir}/tasks/${t}.md`);
    tasksByTeam[t] = task || null;
  }

  await ensureDir(`${workDir}/reviews`);
  await ensureDir(`${workDir}/ssot`);

  const ssotBlocks = [];
  for (const t of teams.slice().sort((a, b) => a.localeCompare(b))) {
    const out = `${workDir}/ssot/SSOT_BUNDLE.team-${t}.json`;
    const res = await resolveSsotBundle({ projectRoot, view: `team:${t}`, outPath: out, dryRun: false });
    if (!res.ok) return { ok: false, message: `Cannot review: SSOT resolution failed for team ${t}: ${res.message}` };
    const txt = await readTextIfExists(out);
    if (!txt) return { ok: false, message: `Cannot review: missing SSOT bundle after resolve: ${out}` };
    const workSsotRes = await ensureWorkSsotBundle({ workId, teamId: t, workDir, teamBundlePath: out, allowOverwriteTeamMismatch: true });
    if (!workSsotRes.ok) return { ok: false, message: `Cannot review: SSOT_BUNDLE.json creation failed for team ${t}: ${workSsotRes.message}` };
    const workSsotText = await readTextIfExists(workSsotRes.outPath);
    if (!workSsotText) return { ok: false, message: `Cannot review: missing SSOT_BUNDLE.json after creation: ${workSsotRes.outPath}` };
    const excerpts = renderSsotExcerptsForLlm({ teamBundleText: txt });
    ssotBlocks.push(
      [
        `=== SSOT_BUNDLE.json (team=${t}; reference-only) ===`,
        workSsotText.trim(),
        "",
        "=== SSOT SECTION EXCERPTS (clipped; cite using SSOT:<section_id>@<sha256>) ===",
        excerpts.trim(),
        "",
      ].join("\n"),
    );
  }

  const proposalsDir = `${workDir}/proposals`;
  // Read proposals by scanning proposal filenames (no glob dependency)
  let proposalNames = [];
  try {
    proposalNames = (await readdir(resolveStatePath(proposalsDir), { withFileTypes: true }))
      .filter((d) => d.isFile() && d.name.endsWith(".md"))
      .map((d) => d.name)
      .sort((a, b) => a.localeCompare(b));
  } catch {
    proposalNames = [];
  }

  const proposalsByTeam = {};
  const missingProposals = [];
  for (const t of teams) {
    const matching = proposalNames.filter((n) => n.startsWith(`${t}__`));
    if (!matching.length) {
      proposalsByTeam[t] = [];
      missingProposals.push(t);
      continue;
    }
    const contents = [];
    for (const name of matching) {
      const text = await readTextIfExists(`${proposalsDir}/${name}`);
      if (text) contents.push({ file: `${proposalsDir}/${name}`, text });
    }
    proposalsByTeam[t] = contents;
  }

  const systemPrompt = readFileSync(resolve(repoRoot, "src/llm/prompts/review.system.txt"), "utf8");
  const schemaJson = readFileSync(resolve(repoRoot, "src/llm/schemas/review.llm-output.schema.json"), "utf8");

  const agentsText = await readTextIfExists("config/AGENTS.json");
  if (!agentsText) return { ok: false, message: "Missing config/AGENTS.json (required). Run: node src/cli.js --agents-generate" };
  let agentsCfg;
  try {
    agentsCfg = JSON.parse(agentsText);
  } catch {
    return { ok: false, message: "Invalid config/AGENTS.json (must be valid JSON)." };
  }
  if (!agentsCfg || agentsCfg.version !== 3 || !Array.isArray(agentsCfg.agents)) {
    return { ok: false, message: "Invalid config/AGENTS.json (expected {version:3, agents:[...]}). Run: node src/cli.js --agents-migrate" };
  }
  if (agentsCfg.agents.some((a) => a && typeof a === "object" && Object.prototype.hasOwnProperty.call(a, "model"))) {
    return { ok: false, message: "AGENTS.json contains legacy key 'model'. Run: node src/cli.js --agents-migrate" };
  }

  const enabled = agentsCfg.agents.filter((a) => a && a.enabled === true);
  const reviewers = enabled
    .filter((a) => String(a.role || "").trim() === "reviewer" && String(a.implementation || "").trim() === "llm")
    .slice()
    .sort((a, b) => String(a.agent_id || "").localeCompare(String(b.agent_id || "")));
  const chosen = reviewers[0] || null;
  if (!chosen) return { ok: false, message: "No enabled reviewer agent configured (role=reviewer, implementation=llm)." };

  const profileKey = typeof chosen.llm_profile === "string" ? chosen.llm_profile.trim() : "";
  if (!profileKey) return { ok: false, message: `Reviewer agent ${String(chosen.agent_id || "")} missing llm_profile.` };

  const { loadLlmProfiles, resolveLlmProfileOrError } = await import("../llm/llm-profiles.js");
  const profilesLoaded = await loadLlmProfiles();
  if (!profilesLoaded.ok) return { ok: false, message: profilesLoaded.message };
  const resolved = resolveLlmProfileOrError({ profiles: profilesLoaded.profiles, profileKey });
  if (!resolved.ok) return { ok: false, message: resolved.message };

  const { createLlmClient } = await import("../llm/client.js");
  const llmClient = createLlmClient({ ...resolved.profile });
  if (!llmClient.ok) return { ok: false, message: llmClient.message };

  const repoContext = await getRepoPathsForWork({ workId });
  const repoRegistryConfigured = repoContext.ok && repoContext.configured;
  const repoRegistryNote = repoRegistryConfigured
    ? "Repo registry configured; scanning selected repo(s) for this work item."
    : "Repo registry not configured; scanning current repo only.";
  const repoRootsScanned = repoRegistryConfigured ? (repoContext.repos || []) : [{ repo_id: "ai-team", abs_path: repoRoot, exists: true }];
  const searchTerms = deriveSearchTerms({ intakeMd, tasksByTeam, proposalsByTeam });
  const scan = hasRg()
    ? scanWithRgInRoots({ repoRoots: repoRootsScanned, terms: searchTerms })
    : await scanFallbackInRoots({ repoRoots: repoRootsScanned, terms: searchTerms });

  const userPrompt = [
    `WorkId: ${workId}`,
    "",
    "You must output JSON that matches this schema exactly:",
    schemaJson.trim(),
    "",
    "Inputs:",
    "",
    ...ssotBlocks,
    "=== INTAKE.md ===",
    intakeMd.trim(),
    "",
    intakeTaggedMd
      ? [
          "=== INTAKE_TAGGED.md (optional) ===",
          intakeTaggedMd.trim(),
          "",
        ].join("\n")
      : "",
    "=== ROUTING.json ===",
    routingJson.trim(),
    "",
    "=== REPO CONTEXT (read-only scan) ===",
    `Repo registry: ${repoRegistryNote}`,
    ...(repoRootsScanned.length
      ? ["Repo roots:", ...repoRootsScanned.map((r) => `- ${String(r.repo_id)}: ${String(r.abs_path || "(unknown path)")} (${r.exists ? "exists" : "missing"})`)]
      : ["Repo roots: (none)"]),
    `Search terms: ${searchTerms.join(", ") || "(none)"}`,
    `Matches (captured): ${scan.total_matches}`,
    ...(scan.hits || []).flatMap((h) => [`- ${h.path}`, ...((h.lines || []).slice(0, 2).map((l) => `  - ${l}`))]),
    "",
    "If scan hits are empty, do NOT invent file paths. Call out missing concrete evidence.",
    "",
    ...teams.flatMap((t) => [
      `=== tasks/${t}.md ===`,
      (tasksByTeam[t] || "(missing)").trim(),
      "",
      `=== proposals for ${t} ===`,
      ...(Array.isArray(proposalsByTeam[t]) && proposalsByTeam[t].length
        ? proposalsByTeam[t].flatMap((p) => [`--- ${p.file} ---`, p.text.trim(), ""])
        : ["(missing proposals for team)", ""]),
    ]),
    "Instructions:",
    "- If a proposal is missing for a team, call it out under 'What is missing / unclear'.",
    "- Focus on concrete missing details, cross-team mismatches, and risks.",
  ]
    .filter(Boolean)
    .join("\n");

  const generated = await generateReviewWithRetries({ llm: llmClient.llm, systemPrompt, userPrompt });
  if (!generated.ok) {
    const last = generated.attempts[generated.attempts.length - 1];
    return { ok: false, message: `Reviewer JSON generation failed: ${last?.error || "unknown"}` };
  }

  const timestamp = nowTs();
  const markdown = renderReviewMarkdown({
    workId,
    timestamp,
    teamsReviewed: teams,
    missingProposals,
    repoRootsScanned,
    review: generated.review,
  });

  const outputPath = `${workDir}/reviews/architect-review.md`;
  const outputJsonPath = `${workDir}/reviews/architect-review.json`;
  const jsonText = jsonStableStringify(generated.review);
  const newHash = sha256Hex(markdown);
  const newJsonHash = sha256Hex(jsonText);
  const existing = await readTextIfExists(outputPath);
  const existingHash = existing ? sha256Hex(existing) : null;
  const existingJson = await readTextIfExists(outputJsonPath);
  const existingJsonHash = existingJson ? sha256Hex(existingJson) : null;

  let action = "review_created";
  if (existingHash) {
    action = existingHash === newHash ? "review_skipped" : "review_updated";
  }

  if (action !== "review_skipped") {
    await writeText(outputPath, markdown);
  }
  if (!existingJsonHash || existingJsonHash !== newJsonHash) {
    await writeText(outputJsonPath, jsonText);
  }

  await appendFile(
    "ai/lane_b/ledger.jsonl",
    JSON.stringify({ timestamp, action, workId, output_path: outputPath, hash: newHash, output_json: outputJsonPath, json_hash: newJsonHash }) + "\n",
  );

  const planText = await readTextIfExists("ai/PLAN.md");
  if (planText) {
    await writeText("ai/PLAN.md", await updatePlanWithOptionalReview(planText));
  }

  return {
    ok: true,
    workId,
    action,
    output_path: outputPath,
    output_json: outputJsonPath,
    hash: newHash,
    json_hash: newJsonHash,
    teams_reviewed: teams,
    missing_proposals: missingProposals,
  };
}
