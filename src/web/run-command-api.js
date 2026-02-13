import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { parseJsonObjectFromText } from "../utils/json-extract.js";
import { getCommandSpec, listCommandRegistry, validateCommandArgs } from "./commandRegistry.js";
import { loadWebConfig, runCli, runCliStream } from "./run-cli.js";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function streamRequested(req) {
  const bodyStream = req?.body?.stream === true;
  const queryStream = String(req?.query?.stream || "").trim() === "1";
  const accept = String(req?.headers?.accept || "");
  return bodyStream || queryStream || accept.includes("text/event-stream");
}

function writeSse(res, event, data) {
  const payload = typeof data === "string" ? data : JSON.stringify(data);
  res.write(`event: ${event}\n`);
  res.write(`data: ${payload}\n\n`);
}

function extractStructuredResult(stdout, stderr) {
  const out = parseJsonObjectFromText(stdout);
  if (out.ok && out.value && typeof out.value === "object") return out.value;
  const err = parseJsonObjectFromText(stderr);
  if (err.ok && err.value && typeof err.value === "object") return err.value;
  return null;
}

async function materializeFileInputs(normalizedArgs) {
  const names = ["input", "input-file"];
  const touched = names.filter((name) => typeof normalizedArgs[name] === "string");
  if (!touched.length) return { args: normalizedArgs, cleanupDirAbs: null };

  const dirAbs = await mkdtemp(join(tmpdir(), "ai-team-web-command-"));
  const cloned = { ...normalizedArgs };
  for (const name of touched) {
    const content = String(cloned[name] || "");
    const suffix = name === "input-file" ? "input-file.txt" : "input.txt";
    const fileAbs = join(dirAbs, suffix);
    await writeFile(fileAbs, content, "utf8");
    cloned[name] = fileAbs;
  }
  return { args: cloned, cleanupDirAbs: dirAbs };
}

function buildCliArgs({ cmd, normalizedArgs, params }) {
  const cliArgs = [cmd];
  for (const param of params) {
    const value = normalizedArgs[param.name];
    if (value === undefined) continue;
    if (param.type === "bool") {
      if (value === true) {
        cliArgs.push(`--${param.name}`);
      } else {
        cliArgs.push(`--${param.name}`, "false");
      }
      continue;
    }
    cliArgs.push(`--${param.name}`, String(value));
  }
  return cliArgs;
}

function isMissingValue(raw) {
  return raw === undefined || raw === null || (typeof raw === "string" && raw.trim() === "");
}

function mergeWithDefaults(spec, argsRaw) {
  const merged = { ...((spec && spec.defaultArgs && typeof spec.defaultArgs === "object") ? spec.defaultArgs : {}), ...argsRaw };
  if (spec?.injectProjectRoot === true) {
    const allowsProjectRoot = Array.isArray(spec.params) && spec.params.some((p) => p?.name === "projectRoot");
    if (allowsProjectRoot && isMissingValue(merged.projectRoot)) {
      merged.projectRoot = loadWebConfig().OPS_ROOT;
    }
  }
  return merged;
}

function ensureRunnableSpec(spec) {
  if (!spec) return { ok: false, status: 400, message: "Unknown command." };
  if (spec.exposeInWebUI !== true) return { ok: false, status: 403, message: "Command is hidden from WebUI." };
  if (spec.lane === "project_admin") return { ok: false, status: 403, message: "Project/admin commands are not allowed in this endpoint." };
  return { ok: true };
}

function buildResultEnvelope({ cmd, normalizedArgs, action }) {
  const structured = extractStructuredResult(action.stdout, action.stderr);
  const result = structured || {
    ok: action.ok,
    exitCode: action.exitCode,
    timedOut: action.timedOut,
    stdout: action.stdout,
    stderr: action.stderr,
    signal: action.signal,
  };
  const commandOk = action.ok && !(structured && structured.ok === false);
  return {
    ok: commandOk,
    cmd,
    args: normalizedArgs,
    result,
  };
}

export function registerRunCommandRoutes(
  app,
  { authMiddleware = null, cliTimeoutMs = 120_000, runCliImpl = runCli, runCliStreamImpl = runCliStream } = {},
) {
  const middleware = typeof authMiddleware === "function" ? authMiddleware : (_req, _res, next) => next();

  app.get("/api/command-registry", middleware, async (req, res) => {
    const webOnly = String(req.query?.webOnly || "").trim() === "1";
    return res.status(200).json({
      ok: true,
      commands: listCommandRegistry({ webOnly }),
    });
  });

  app.post("/api/run-command", middleware, async (req, res) => {
    const cmd = typeof req.body?.cmd === "string" ? req.body.cmd.trim() : "";
    const argsRaw = isPlainObject(req.body?.args) ? req.body.args : {};
    const spec = getCommandSpec(cmd);

    const runnable = ensureRunnableSpec(spec);
    if (!runnable.ok) return res.status(runnable.status).json({ ok: false, message: runnable.message });

    const argsWithDefaults = mergeWithDefaults(spec, argsRaw);
    const validated = validateCommandArgs(spec, argsWithDefaults);
    if (!validated.ok) return res.status(400).json({ ok: false, message: validated.message });

    const params = Array.isArray(spec.params) ? spec.params : [];
    const materialized = await materializeFileInputs(validated.normalized);
    const normalizedArgs = materialized.args;
    const cliArgs = buildCliArgs({ cmd, normalizedArgs, params });
    const wantsStream = streamRequested(req);

    if (!wantsStream) {
      try {
        const action = await runCliImpl({ args: cliArgs, timeoutMs: cliTimeoutMs });
        return res.status(200).json(buildResultEnvelope({ cmd, normalizedArgs: validated.normalized, action }));
      } catch (err) {
        return res.status(500).json({ ok: false, cmd, args: validated.normalized, message: err instanceof Error ? err.message : String(err) });
      } finally {
        if (materialized.cleanupDirAbs) await rm(materialized.cleanupDirAbs, { recursive: true, force: true });
      }
    }

    res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders?.();
    writeSse(res, "start", { cmd, args: validated.normalized });

    try {
      const action = await runCliStreamImpl({
        args: cliArgs,
        timeoutMs: cliTimeoutMs,
        onStdout: (chunk) => writeSse(res, "stdout", { chunk }),
        onStderr: (chunk) => writeSse(res, "stderr", { chunk }),
      });
      writeSse(res, "done", buildResultEnvelope({ cmd, normalizedArgs: validated.normalized, action }));
      return res.end();
    } catch (err) {
      writeSse(res, "error", { ok: false, cmd, args: validated.normalized, message: err instanceof Error ? err.message : String(err) });
      return res.end();
    } finally {
      if (materialized.cleanupDirAbs) await rm(materialized.cleanupDirAbs, { recursive: true, force: true });
    }
  });
}
