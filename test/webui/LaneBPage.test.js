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
  app.get("/lane-b", (_req, res) => res.sendFile(resolve("src/web/public/lane-b.html")));
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

test("Lane B page exposes tabs and registry-driven workflow ordering", async (t) => {
  const { server, baseUrl } = await startApp();
  t.after(() => server.close());

  const pageRes = await fetch(`${baseUrl}/lane-b`);
  const html = await pageRes.text();
  assert.equal(pageRes.status, 200);
  assert.match(html, /lane:\s*"lane_b"/);
  assert.match(html, /"Status", "Intake", "Triage", "Approvals", "Work Items"/);

  const regRes = await fetch(`${baseUrl}/api/command-registry?webOnly=1`);
  const regJson = await regRes.json();
  const workflowCommands = regJson.commands
    .filter((cmd) => cmd.lane === "lane_b" && cmd.group === "Lane B Workflow")
    .sort(cmpRegistry)
    .map((cmd) => cmd.cmd);

  assertOrderedSubset(workflowCommands, [
    "--sweep",
    "--triage",
    "--propose",
    "--plan-approval",
    "--plan-approve",
    "--plan-reject",
    "--qa-obligations",
    "--apply-approval",
    "--apply-approve",
    "--apply-reject",
    "--apply",
    "--watchdog",
    "--merge-approval",
    "--merge-approve",
    "--merge-reject",
  ]);
});

test("Lane B command calls validate params before execution", async (t) => {
  const { server, baseUrl, calls } = await startApp();
  t.after(() => server.close());

  const missingRes = await fetch(`${baseUrl}/api/run-command`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ cmd: "--plan-approval", args: {} }),
  });
  const missingJson = await missingRes.json();
  assert.equal(missingRes.status, 400);
  assert.match(String(missingJson.message || ""), /Missing required arg 'workId'/);

  const okRes = await fetch(`${baseUrl}/api/run-command`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ cmd: "--plan-approval", args: { workId: "W-100" } }),
  });
  const okJson = await okRes.json();
  assert.equal(okRes.status, 200);
  assert.equal(okJson.ok, true);
  assert.deepEqual(calls[calls.length - 1], ["--plan-approval", "--workId", "W-100"]);
});
