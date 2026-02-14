import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { resolve } from "node:path";

import { registerRunCommandRoutes } from "../../src/web/run-command-api.js";

function cmpRegistry(a, b) {
  const groupCmp = String(a?.group || "").localeCompare(String(b?.group || ""));
  if (groupCmp !== 0) return groupCmp;
  const orderCmp = Number(a?.order || 0) - Number(b?.order || 0);
  if (orderCmp !== 0) return orderCmp;
  return String(a?.cmd || "").localeCompare(String(b?.cmd || ""));
}

function assertOrderedSubset(all, subset) {
  let cursor = 0;
  for (const item of all) {
    if (item === subset[cursor]) cursor += 1;
    if (cursor === subset.length) break;
  }
  assert.equal(cursor, subset.length, `Expected ordered subset ${subset.join(", ")} in ${all.join(", ")}`);
}

async function startApp() {
  const app = express();
  const calls = [];
  app.use(express.json({ limit: "64kb" }));
  app.use("/static", express.static(resolve("src/web/public"), { fallthrough: false }));
  app.get("/lane-a", (_req, res) => res.sendFile(resolve("src/web/public/lane-a.html")));
  app.get("/api/project", (_req, res) => res.json({ ok: true, project_key: "demo" }));

  registerRunCommandRoutes(app, {
    runCliImpl: async ({ args }) => {
      calls.push(args);
      return {
        ok: true,
        exitCode: 0,
        timedOut: false,
        signal: null,
        stdout: `${JSON.stringify({ ok: true, cmd: args[0], args })}\n`,
        stderr: "",
      };
    },
    runCliStreamImpl: async ({ args }) => ({
      ok: true,
      exitCode: 0,
      timedOut: false,
      signal: null,
      stdout: `${JSON.stringify({ ok: true, cmd: args[0], args })}\n`,
      stderr: "",
    }),
  });

  const server = await new Promise((resolvePromise) => {
    const s = app.listen(0, "127.0.0.1", () => resolvePromise(s));
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : null;
  return { server, baseUrl: `http://127.0.0.1:${port}`, calls };
}

test("Lane A page exposes tabs and uses registry workflow ordering", async (t) => {
  const { server, baseUrl } = await startApp();
  t.after(() => server.close());

  const pageRes = await fetch(`${baseUrl}/lane-a`);
  const html = await pageRes.text();
  assert.equal(pageRes.status, 200);
  assert.match(html, /lane:\s*"lane_a"/);
  assert.match(html, /"Status", "Interview", "Committee", "Meetings", "Approvals", "Skills"/);

  const regRes = await fetch(`${baseUrl}/api/command-registry?webOnly=1`);
  const regJson = await regRes.json();
  const statusCommands = regJson.commands
    .filter((cmd) => cmd.lane === "lane_a" && cmd.tab === "Status")
    .sort(cmpRegistry)
    .map((cmd) => cmd.cmd);

  assertOrderedSubset(statusCommands, [
    "--knowledge-index",
    "--knowledge-scan",
    "--knowledge-kickoff-reverse",
    "--knowledge-committee",
    "--knowledge-sufficiency",
    "--knowledge-confirm-v1",
    "--knowledge-kickoff-forward",
  ]);

  const skillCommands = regJson.commands
    .filter((cmd) => cmd.lane === "lane_a" && cmd.tab === "Skills")
    .sort(cmpRegistry)
    .map((cmd) => cmd.cmd);
  assertOrderedSubset(skillCommands, [
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
  ]);
});

test("Lane A command button payloads execute through /api/run-command", async (t) => {
  const { server, baseUrl, calls } = await startApp();
  t.after(() => server.close());

  const runRes = await fetch(`${baseUrl}/api/run-command`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ cmd: "--knowledge-status", args: { json: true } }),
  });
  const runJson = await runRes.json();
  assert.equal(runRes.status, 200);
  assert.equal(runJson.ok, true);
  assert.equal(runJson.cmd, "--knowledge-status");
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0], ["--knowledge-status", "--json"]);
});
