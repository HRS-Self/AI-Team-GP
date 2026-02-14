import { maybeAugmentLlmMessagesWithSkills } from "../../llm/prompt-augment.js";

function clipText(text, maxChars) {
  const s = String(text || "");
  if (s.length <= maxChars) return s;
  return s.slice(0, maxChars) + "\n\n[...clipped...]\n";
}

function isStringArray(x) {
  return Array.isArray(x) && x.every((v) => typeof v === "string");
}

function renderEvidenceSection({ workId, evidence }) {
  const e = evidence || {};
  const artifacts = Array.isArray(e.artifacts) ? e.artifacts : [];
  const repoRoots = Array.isArray(e.repo_roots) ? e.repo_roots : [];
  const repoRegistryNote = typeof e.repo_registry_note === "string" ? e.repo_registry_note.trim() : "";
  const searchTerms = Array.isArray(e.search_terms) ? e.search_terms : [];
  const totalMatches = Number.isFinite(e.total_matches) ? e.total_matches : 0;
  const hits = Array.isArray(e.hits) ? e.hits : [];
  const validationCommands = Array.isArray(e.validation_commands) ? e.validation_commands : [];

  const lines = [];
  lines.push("## Evidence reviewed");
  lines.push("");
  lines.push("Artifacts read:");
  if (!artifacts.length) lines.push("- (none)");
  for (const p of artifacts) lines.push(`- \`${String(p)}\``);
  lines.push("");
  lines.push("Repo registry:");
  lines.push(`- ${repoRegistryNote || "Repo registry not configured; scanning current repo only."}`);
  lines.push("");
  lines.push("Repo roots scanned:");
  if (!repoRoots.length) {
    lines.push("- (none)");
  } else {
    for (const r of repoRoots) {
      const id = String(r?.repo_id || "").trim() || "unknown";
      const p = String(r?.abs_path || "").trim() || "(unknown path)";
      const exists = typeof r?.exists === "boolean" ? (r.exists ? "exists" : "missing") : "unknown";
      lines.push(`- \`${id}\`: \`${p}\` (${exists})`);
    }
  }
  lines.push("");
  lines.push("Repo scan:");
  lines.push(`- Search terms: ${searchTerms.length ? searchTerms.map((t) => `\`${t}\``).join(", ") : "(none)"}`);
  lines.push(`- Matches (captured): ${totalMatches}`);
  lines.push(`- Validation commands (discovered): ${validationCommands.length ? validationCommands.map((c) => `\`${c}\``).join(", ") : "(none)"}`);
  lines.push("");
  lines.push("File hits (up to 10 files; up to 25 lines total):");
  if (!hits.length) {
    lines.push("- No matches found.");
    lines.push("");
    return lines;
  }

  for (const hit of hits) {
    const path = String(hit?.path || "").trim();
    if (!path) continue;
    lines.push(`- \`${path}\``);
    const lns = Array.isArray(hit?.lines) ? hit.lines : [];
    for (const ln of lns) {
      lines.push(`  - ${String(ln)}`);
    }
  }
  lines.push("");
  return lines;
}

export function validateProposalJson(obj) {
  const errors = [];

  const requiredString = (k) => {
    if (typeof obj?.[k] !== "string" || !obj[k].trim()) errors.push(`Missing/invalid ${k} (expected non-empty string).`);
  };
  const requiredStringArray = (k) => {
    if (!isStringArray(obj?.[k])) errors.push(`Missing/invalid ${k} (expected string[]).`);
  };

  if (!Array.isArray(obj?.ssot_references)) {
    errors.push("Missing/invalid ssot_references (expected array).");
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

  requiredString("summary");
  requiredStringArray("understanding_of_requirements");
  requiredStringArray("proposed_approach");
  requiredStringArray("likely_files_or_areas_impacted");
  requiredStringArray("risks_and_mitigations");
  requiredStringArray("questions_or_clarifications_needed");
  requiredStringArray("suggested_validation");

  return { ok: errors.length === 0, errors };
}

export function proposalJsonToMarkdown({ workId, teamId, agentId, proposal, evidence, validationFailureNote, rawModelOutput }) {
  const lines = [];

  lines.push(`# Proposal: ${teamId} / ${agentId}`);
  lines.push("");
  lines.push(`Work item: ${workId}`);
  lines.push("");

  lines.push(...renderEvidenceSection({ workId, evidence }));

  if (validationFailureNote) {
    lines.push("## Validation failed");
    lines.push("");
    lines.push(validationFailureNote);
    lines.push("");
  }

  lines.push("## Summary");
  lines.push("");
  lines.push(proposal?.summary ? String(proposal.summary).trim() : "(unavailable)");
  lines.push("");

  const section = (title, arr) => {
    lines.push(`## ${title}`);
    lines.push("");
    if (!Array.isArray(arr) || arr.length === 0) {
      lines.push("- (none)");
    } else {
      for (const item of arr) lines.push(`- ${String(item)}`);
    }
    lines.push("");
  };

  section("Understanding of requirements", proposal?.understanding_of_requirements);
  section("Proposed approach", proposal?.proposed_approach);
  section("Likely files/areas impacted", proposal?.likely_files_or_areas_impacted);
  section("Risks & mitigations", proposal?.risks_and_mitigations);
  section("Questions / clarifications needed", proposal?.questions_or_clarifications_needed);
  section("Suggested validation (tests to run)", proposal?.suggested_validation);

  if (rawModelOutput) {
    lines.push("## Raw model output");
    lines.push("");
    lines.push("```");
    lines.push(clipText(rawModelOutput, 12000).trimEnd());
    lines.push("```");
    lines.push("");
  }

  return lines.join("\n");
}

export async function generateProposalWithRetries({ llm, systemPrompt, userPrompt }) {
  const attempts = [];
  const baseMessages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: userPrompt },
  ];
  const augmented = await maybeAugmentLlmMessagesWithSkills({
    baseMessages,
    projectRoot: process.env.AI_PROJECT_ROOT || null,
    input: {
      scope: "system",
      base_system: String(systemPrompt || ""),
      base_prompt: String(userPrompt || ""),
      context: { role: "lane_b.proposal" },
      constraints: { output: "json_only" },
      knowledge_snippets: [],
    },
  });
  const invokeMessages = augmented.messages;

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const response = await llm.invoke(invokeMessages);

    const raw = typeof response?.content === "string" ? response.content : String(response?.content ?? "");
    let parsed = null;
    let parseError = null;

    try {
      parsed = JSON.parse(raw);
    } catch (err) {
      parseError = err instanceof Error ? err.message : String(err);
    }

    if (parsed) {
      const v = validateProposalJson(parsed);
      if (v.ok) {
        return { ok: true, proposal: parsed, rawModelOutput: raw, attempts: attempt };
      }
      attempts.push({ attempt, raw, error: `Schema validation failed: ${v.errors.join(" ")}` });
    } else {
      attempts.push({ attempt, raw, error: `JSON parse failed: ${parseError}` });
    }
  }

  return { ok: false, attempts };
}
