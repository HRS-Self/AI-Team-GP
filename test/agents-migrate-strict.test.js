import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { runAgentsMigrate } from "../src/project/agents-migrate.js";

test("--agents-migrate refuses unknown LLM roles (no implicit fallback)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-agents-migrate-"));
  process.env.AI_PROJECT_ROOT = join(root, "ops");

  mkdirSync(join(root, "ops", "config"), { recursive: true });

  const agentsPath = join(root, "ops", "config", "AGENTS.json");
  const ledgerPath = join(root, "ops", "ai", "lane_b", "ledger.jsonl");

  writeFileSync(
    agentsPath,
    JSON.stringify(
      {
        version: 3,
        agents: [
          {
            agent_id: "FrontendDP__planner__01",
            team_id: "FrontendDP",
            role: "planner",
            implementation: "llm",
            enabled: true,
            capacity: 1,
            model: "legacy-model",
          },
          {
            agent_id: "Weird__agent__01",
            team_id: "FrontendDP",
            role: "mystery_role",
            implementation: "llm",
            enabled: true,
            capacity: 1,
          },
        ],
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  const before = readFileSync(agentsPath, "utf8");
  const res = await runAgentsMigrate();
  assert.equal(res.ok, false);
  assert.ok(String(res.message).includes("unknown role"), res.message);
  assert.ok(Array.isArray(res.unknown_role_agents) && res.unknown_role_agents.length === 1);

  const after = readFileSync(agentsPath, "utf8");
  assert.equal(after, before, "AGENTS.json must not be rewritten on failed migration");
  assert.equal(existsSync(ledgerPath), false, "ledger must not be written on failed migration");
});
