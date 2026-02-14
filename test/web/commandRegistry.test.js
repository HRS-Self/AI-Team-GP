import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { commandRegistry } from "../../src/web/commandRegistry.js";

function expectedCommandsFromUsage() {
  const text = readFileSync("src/cli/entry.js", "utf8");
  const fromUsage = new Set([...text.matchAll(/node src\/cli\.js (--[a-z0-9-]+)/g)].map((m) => m[1]));
  fromUsage.delete("--dry-run");
  fromUsage.add("--knowledge-extract-tasks");
  return fromUsage;
}

test("command registry contains all CLI command entries", () => {
  const expected = expectedCommandsFromUsage();
  const actual = new Set(Object.keys(commandRegistry));
  assert.equal(actual.size, expected.size);
  assert.deepEqual([...actual].sort((a, b) => a.localeCompare(b)), [...expected].sort((a, b) => a.localeCompare(b)));
});

test("command registry exposes expected WebUI commands only", () => {
  const exposed = Object.values(commandRegistry).filter((entry) => entry.exposeInWebUI === true);
  assert.ok(exposed.length >= 20);
  assert.ok(exposed.every((entry) => entry.lane !== "project_admin"));

  const exposedSet = new Set(exposed.map((entry) => entry.cmd));
  const mustInclude = [
    "--knowledge-index",
    "--knowledge-scan",
    "--knowledge-kickoff-reverse",
    "--knowledge-committee",
    "--knowledge-sufficiency",
    "--knowledge-kickoff-forward",
    "--sweep",
    "--triage",
    "--propose",
    "--plan-approval",
    "--qa-obligations",
    "--apply",
    "--watchdog",
    "--merge-approval",
    "--lane-a-to-lane-b",
    "--gaps-to-intake",
    "--lane-b-events-list",
    "--knowledge-events-status",
    "--ssot-drift-check",
    "--project-skills-status",
    "--skills-list",
    "--skills-show",
    "--project-skills-allow",
    "--project-skills-deny",
    "--skills-draft",
    "--skills-refresh",
    "--skills-governance",
    "--skills-approve",
    "--skills-reject",
  ];
  for (const cmd of mustInclude) assert.equal(exposedSet.has(cmd), true, `Missing exposed command ${cmd}`);

  const mustNotExpose = ["--initial-project", "--list-projects", "--show-project-detail", "--remove-project", "--migrate-project-layout"];
  for (const cmd of mustNotExpose) assert.equal(exposedSet.has(cmd), false, `Unexpected exposed command ${cmd}`);
});
