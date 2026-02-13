import test from "node:test";
import assert from "node:assert/strict";
import express from "express";

import { registerRunCommandRoutes } from "../../src/web/run-command-api.js";

async function startTestApp() {
  const app = express();
  app.use(express.json({ limit: "64kb" }));
  registerRunCommandRoutes(app, {
    runCliImpl: async () => ({
      ok: true,
      exitCode: 0,
      timedOut: false,
      signal: null,
      stdout: "{\"ok\":true}\n",
      stderr: "",
    }),
    runCliStreamImpl: async () => ({
      ok: true,
      exitCode: 0,
      timedOut: false,
      signal: null,
      stdout: "{\"ok\":true}\n",
      stderr: "",
    }),
  });
  const server = await new Promise((resolvePromise) => {
    const s = app.listen(0, "127.0.0.1", () => resolvePromise(s));
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : null;
  return { server, baseUrl: `http://127.0.0.1:${port}` };
}

async function postJson(baseUrl, body) {
  const response = await fetch(`${baseUrl}/api/run-command`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = await response.json().catch(() => ({}));
  return { response, json };
}

test("run-command validates integer params", async (t) => {
  const { server, baseUrl } = await startTestApp();
  t.after(() => server.close());

  const { response, json } = await postJson(baseUrl, {
    cmd: "--triage",
    args: { limit: "bad-int" },
  });

  assert.equal(response.status, 400);
  assert.equal(json.ok, false);
  assert.match(String(json.message || ""), /must be an integer/i);
});

test("run-command validates boolean params", async (t) => {
  const { server, baseUrl } = await startTestApp();
  t.after(() => server.close());

  const { response, json } = await postJson(baseUrl, {
    cmd: "--watchdog",
    args: { "watchdog-ci": "not-bool" },
  });

  assert.equal(response.status, 400);
  assert.equal(json.ok, false);
  assert.match(String(json.message || ""), /must be boolean/i);
});

test("run-command rejects unexpected args", async (t) => {
  const { server, baseUrl } = await startTestApp();
  t.after(() => server.close());

  const { response, json } = await postJson(baseUrl, {
    cmd: "--sweep",
    args: { unexpected: 1 },
  });

  assert.equal(response.status, 400);
  assert.equal(json.ok, false);
  assert.match(String(json.message || ""), /Unexpected arg/i);
});

test("run-command enforces required string params", async (t) => {
  const { server, baseUrl } = await startTestApp();
  t.after(() => server.close());

  const { response, json } = await postJson(baseUrl, {
    cmd: "--knowledge-kickoff-reverse",
    args: { start: true, "non-interactive": true, scope: "system" },
  });

  assert.equal(response.status, 400);
  assert.equal(json.ok, false);
  assert.match(String(json.message || ""), /Missing required arg 'input-file'/);
});
