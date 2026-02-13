import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { isAbsolute, resolve } from "node:path";

import { resolveOpsRootAbs } from "../paths/project-paths.js";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function readRequiredEnv(name) {
  const v = typeof process.env[name] === "string" ? process.env[name].trim() : "";
  if (!v) throw new Error(`Missing required env var ${name}.`);
  return v;
}

export function loadWebConfig() {
  const AI_TEAM_REPO = (typeof process.env.AI_TEAM_REPO === "string" && process.env.AI_TEAM_REPO.trim()) || "/opt/GitRepos/AI-Team";
  const CLI_PATH = (typeof process.env.CLI_PATH === "string" && process.env.CLI_PATH.trim()) || `${AI_TEAM_REPO}/src/cli.js`;
  const OPS_ROOT = resolveOpsRootAbs({ projectRoot: readRequiredEnv("AI_PROJECT_ROOT"), required: true });

  const repoAbs = resolve(AI_TEAM_REPO);
  const cliAbs = resolve(CLI_PATH);

  if (!isAbsolute(OPS_ROOT)) throw new Error("AI_PROJECT_ROOT must be an absolute path.");
  if (!existsSync(cliAbs)) throw new Error(`CLI_PATH does not exist: ${cliAbs}`);
  if (!existsSync(repoAbs)) throw new Error(`AI_TEAM_REPO does not exist: ${repoAbs}`);

  return { AI_TEAM_REPO: repoAbs, CLI_PATH: cliAbs, OPS_ROOT };
}

export async function runCli({ args, timeoutMs = 120_000, extraEnv = null } = {}) {
  if (!Array.isArray(args) || args.some((a) => typeof a !== "string")) throw new Error("runCli args must be string[]");
  const cfg = loadWebConfig();

  const env = {
    ...process.env,
    AI_PROJECT_ROOT: cfg.OPS_ROOT,
    ...(isPlainObject(extraEnv) ? extraEnv : {}),
  };

  return await new Promise((resolvePromise) => {
    const child = spawn(process.execPath, [cfg.CLI_PATH, ...args], {
      cwd: cfg.AI_TEAM_REPO,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let timedOut = false;

    const killTimer =
      Number.isFinite(Number(timeoutMs)) && Number(timeoutMs) > 0
        ? setTimeout(() => {
            timedOut = true;
            try {
              child.kill("SIGKILL");
            } catch {
              // ignore
            }
          }, Number(timeoutMs))
        : null;

    child.stdout.on("data", (b) => {
      stdout += b.toString("utf8");
    });
    child.stderr.on("data", (b) => {
      stderr += b.toString("utf8");
    });

    child.on("close", (code, signal) => {
      if (killTimer) clearTimeout(killTimer);
      resolvePromise({
        ok: code === 0 && !timedOut,
        exitCode: typeof code === "number" ? code : null,
        signal: signal || null,
        timedOut,
        stdout,
        stderr,
      });
    });
  });
}

export async function runCliStream({
  args,
  timeoutMs = 120_000,
  extraEnv = null,
  onStdout = null,
  onStderr = null,
} = {}) {
  if (!Array.isArray(args) || args.some((a) => typeof a !== "string")) throw new Error("runCliStream args must be string[]");
  const cfg = loadWebConfig();

  const env = {
    ...process.env,
    AI_PROJECT_ROOT: cfg.OPS_ROOT,
    ...(isPlainObject(extraEnv) ? extraEnv : {}),
  };

  return await new Promise((resolvePromise) => {
    const child = spawn(process.execPath, [cfg.CLI_PATH, ...args], {
      cwd: cfg.AI_TEAM_REPO,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let timedOut = false;

    const killTimer =
      Number.isFinite(Number(timeoutMs)) && Number(timeoutMs) > 0
        ? setTimeout(() => {
            timedOut = true;
            try {
              child.kill("SIGKILL");
            } catch {
              // ignore
            }
          }, Number(timeoutMs))
        : null;

    child.stdout.on("data", (buffer) => {
      const chunk = buffer.toString("utf8");
      stdout += chunk;
      if (typeof onStdout === "function") onStdout(chunk);
    });
    child.stderr.on("data", (buffer) => {
      const chunk = buffer.toString("utf8");
      stderr += chunk;
      if (typeof onStderr === "function") onStderr(chunk);
    });

    child.on("close", (code, signal) => {
      if (killTimer) clearTimeout(killTimer);
      resolvePromise({
        ok: code === 0 && !timedOut,
        exitCode: typeof code === "number" ? code : null,
        signal: signal || null,
        timedOut,
        stdout,
        stderr,
      });
    });
  });
}
