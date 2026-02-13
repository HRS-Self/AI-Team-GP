import { readTextIfExists, writeText, appendFile } from "../utils/fs.js";
import { nowTs } from "../utils/id.js";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function roleToProfile(role) {
  const r = String(role || "").trim();
  if (r === "planner") return "planner.code_generation";
  if (r === "reviewer") return "architect.reviewer";
  if (r === "interviewer") return "architect.interviewer";
  if (r === "writer") return "tech.writer";
  if (r === "pr_description") return "pr.description";
  return null;
}

export async function runAgentsMigrate() {
  const path = "config/AGENTS.json";
  const text = await readTextIfExists(path);
  if (!text) return { ok: false, message: `Missing ${path}. Run --agents-generate first.` };

  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { ok: false, message: `Invalid JSON in ${path}.` };
  }

  if (!isPlainObject(parsed) || !Array.isArray(parsed.agents)) return { ok: false, message: `Invalid ${path}: expected { version, agents:[...] }.` };

  const outAgents = [];
  let migratedCount = 0;
  let removedModelCount = 0;
  const unknownRoleAgents = [];

  for (const a of parsed.agents) {
    if (!isPlainObject(a)) continue;
    const next = { ...a };

    const impl = String(next.implementation || "").trim();
    if (impl === "llm") {
      const existing = typeof next.llm_profile === "string" ? next.llm_profile.trim() : "";
      if (!existing) {
        const prof = roleToProfile(next.role);
        if (!prof) {
          unknownRoleAgents.push({ agent_id: String(next.agent_id || "").trim() || "<unknown>", role: String(next.role || "").trim() || "<missing>" });
        } else {
          next.llm_profile = prof;
          migratedCount += 1;
        }
      }
      if (Object.prototype.hasOwnProperty.call(next, "model")) {
        delete next.model;
        removedModelCount += 1;
      }
    } else {
      if (Object.prototype.hasOwnProperty.call(next, "model")) delete next.model;
    }

    outAgents.push(next);
  }

  if (unknownRoleAgents.length) {
    return {
      ok: false,
      message:
        "Cannot migrate AGENTS.json: some LLM agents are missing llm_profile and have an unknown role. " +
        "Set llm_profile manually for these agents, or fix their role, then rerun --agents-migrate.",
      unknown_role_agents: unknownRoleAgents,
    };
  }

  const nextConfig = { version: 3, agents: outAgents };
  await writeText(path, JSON.stringify(nextConfig, null, 2) + "\n");
  await appendFile(
    "ai/lane_b/ledger.jsonl",
    JSON.stringify({ timestamp: nowTs(), action: "agents_migrated", written: path, migrated_llm_profile_count: migratedCount, removed_model_count: removedModelCount }) + "\n",
  );

  return { ok: true, written: path, migrated_llm_profile_count: migratedCount, removed_model_count: removedModelCount, agent_count: outAgents.length };
}
