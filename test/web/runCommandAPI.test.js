import test from "node:test";
import assert from "node:assert/strict";
import express from "express";

import { registerRunCommandRoutes } from "../../src/web/run-command-api.js";

function makeStubRunCli() {
  return async ({ args }) => {
    if (args[0] === "--triage") {
      return {
        ok: true,
        exitCode: 0,
        timedOut: false,
        signal: null,
        stdout: `${JSON.stringify({ ok: true, action: "triage", limit: Number(args[2]) })}\n`,
        stderr: "",
      };
    }
    return {
      ok: true,
      exitCode: 0,
      timedOut: false,
      signal: null,
      stdout: `${JSON.stringify({ ok: true, command: args[0] })}\n`,
      stderr: "",
    };
  };
}

async function startTestApp() {
  const app = express();
  app.use(express.json({ limit: "64kb" }));
  registerRunCommandRoutes(app, {
    runCliImpl: makeStubRunCli(),
    runCliStreamImpl: makeStubRunCli(),
  });
  const server = await new Promise((resolvePromise) => {
    const s = app.listen(0, "127.0.0.1", () => resolvePromise(s));
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : null;
  return { server, baseUrl: `http://127.0.0.1:${port}` };
}

async function postJson(baseUrl, path, body) {
  const response = await fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = await response.json().catch(() => ({}));
  return { response, json };
}

test("run-command executes valid exposed command and returns structured result", async (t) => {
  const { server, baseUrl } = await startTestApp();
  t.after(() => server.close());

  const { response, json } = await postJson(baseUrl, "/api/run-command", {
    cmd: "--triage",
    args: { limit: 7 },
  });

  assert.equal(response.status, 200);
  assert.equal(json.ok, true);
  assert.equal(json.cmd, "--triage");
  assert.equal(json.args.limit, 7);
  assert.equal(json.result.action, "triage");
  assert.equal(json.result.limit, 7);
});

test("run-command rejects unknown command", async (t) => {
  const { server, baseUrl } = await startTestApp();
  t.after(() => server.close());

  const { response, json } = await postJson(baseUrl, "/api/run-command", {
    cmd: "--not-a-command",
    args: {},
  });

  assert.equal(response.status, 400);
  assert.equal(json.ok, false);
});

test("run-command rejects missing required params", async (t) => {
  const { server, baseUrl } = await startTestApp();
  t.after(() => server.close());

  const { response, json } = await postJson(baseUrl, "/api/run-command", {
    cmd: "--knowledge-confirm-v1",
    args: {},
  });

  assert.equal(response.status, 400);
  assert.equal(json.ok, false);
  assert.match(String(json.message || ""), /Missing required arg 'by'/);
});

test("run-command rejects hidden commands", async (t) => {
  const { server, baseUrl } = await startTestApp();
  t.after(() => server.close());

  const { response, json } = await postJson(baseUrl, "/api/run-command", {
    cmd: "--initial-project",
    args: {},
  });

  assert.equal(response.status, 403);
  assert.equal(json.ok, false);
});
