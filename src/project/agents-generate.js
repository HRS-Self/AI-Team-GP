import { createInterface } from "node:readline/promises";
import { stdin, stdout } from "node:process";

import { readTextIfExists, writeText, appendFile } from "../utils/fs.js";
import { nowTs } from "../utils/id.js";
import { generateAgentsConfig, validateAgentsConfigCoversTeams, agentsConfigToText } from "./agents-generator.js";

function parsePositiveIntOrNull(raw) {
  const s = typeof raw === "string" ? raw.trim() : "";
  if (!s) return null;
  const n = Number.parseInt(s, 10);
  if (!Number.isFinite(n) || n < 1) return null;
  return n;
}

async function promptYesNoDefault(rl, question, defaultYes) {
  const suffix = defaultYes ? " (Y/n)" : " (y/N)";
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const raw = await rl.question(`${question}${suffix}`);
    const v = String(raw || "").trim().toLowerCase();
    if (!v) return defaultYes;
    if (["y", "yes"].includes(v)) return true;
    if (["n", "no"].includes(v)) return false;
  }
}

export async function runAgentsGenerate({ nonInteractive = false } = {}) {
  const teamsText = await readTextIfExists("config/TEAMS.json");
  if (!teamsText) return { ok: false, message: "Missing config/TEAMS.json (required). Run --initial-project first." };

  let teamsConfig;
  try {
    teamsConfig = JSON.parse(teamsText);
  } catch {
    return { ok: false, message: "Invalid config/TEAMS.json (must be valid JSON)." };
  }

  const teams = Array.isArray(teamsConfig?.teams) ? teamsConfig.teams : [];
  const teamIds = teams.map((t) => String(t?.team_id || "").trim()).filter(Boolean);
  if (!teamIds.length) return { ok: false, message: "config/TEAMS.json has no teams[]." };

  let plannersPerTeam = 1;
  let createApplierPerTeam = true;

  if (!nonInteractive) {
    const rl = createInterface({ input: stdin, output: stdout });
    try {
      const rawCount = await rl.question("How many LLM planners per team? (default 1):");
      plannersPerTeam = parsePositiveIntOrNull(rawCount) || 1;

      createApplierPerTeam = await promptYesNoDefault(rl, "Create code applier per team?", true);
    } finally {
      rl.close();
    }
  }

  const config = generateAgentsConfig({ teamsConfig, plannersPerTeam, createApplierPerTeam });
  const validated = validateAgentsConfigCoversTeams({ teamsConfig, agentsConfig: config });
  if (!validated.ok) return { ok: false, message: "Generated AGENTS.json did not validate.", errors: validated.errors };

  await writeText("config/AGENTS.json", agentsConfigToText(config));
  await appendFile(
    "ai/lane_b/ledger.jsonl",
    JSON.stringify({
      timestamp: nowTs(),
      action: "agents_generated",
      team_count: teamIds.length,
      planners_per_team: plannersPerTeam,
      create_applier_per_team: createApplierPerTeam,
    }) + "\n",
  );

  return {
    ok: true,
    written: "config/AGENTS.json",
    team_count: teamIds.length,
    planners_per_team: plannersPerTeam,
    create_applier_per_team: createApplierPerTeam,
    non_interactive: !!nonInteractive,
  };
}
