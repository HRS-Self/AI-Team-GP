import express from "express";
import bcrypt from "bcryptjs";
import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, open, readdir } from "node:fs/promises";
import { isAbsolute, resolve, join, relative, basename } from "node:path";

import { loadWebConfig } from "./run-cli.js";
import { loadProjectPaths } from "../paths/project-paths.js";
import { loadRegistry, listProjects, getProject as getRegistryProject } from "../registry/project-registry.js";
import { registerLaneAHealthRoutes } from "./lane-a-health.js";
import { registerRunCommandRoutes } from "./run-command-api.js";
import { registerStatusOverviewRoutes } from "./status-overview.js";

function nowMs() {
  return Date.now();
}

function nowISO() {
  return new Date().toISOString();
}

function readEnv(name, { required = false, fallback = null } = {}) {
  const v = typeof process.env[name] === "string" ? process.env[name].trim() : "";
  if (!v && required) throw new Error(`Missing required env var ${name}.`);
  return v || fallback;
}

function mustAbsPath(name, raw) {
  if (!raw) throw new Error(`Missing required env var ${name}.`);
  if (!isAbsolute(raw)) throw new Error(`${name} must be an absolute path (got: ${raw}).`);
  return resolve(raw);
}

function parseCookies(header) {
  const out = {};
  const h = String(header || "");
  for (const part of h.split(";")) {
    const idx = part.indexOf("=");
    if (idx <= 0) continue;
    const k = part.slice(0, idx).trim();
    const v = part.slice(idx + 1).trim();
    if (k) out[k] = v;
  }
  return out;
}

function signSid(secret, sid) {
  return createHmac("sha256", secret).update(String(sid), "utf8").digest("hex");
}

function safeEqualHex(a, b) {
  const aa = Buffer.from(String(a || ""), "utf8");
  const bb = Buffer.from(String(b || ""), "utf8");
  if (aa.length !== bb.length) return false;
  return timingSafeEqual(aa, bb);
}

function makeCookie({ name, value, httpOnly = true, sameSite = "Strict", secure = false, path = "/", maxAgeSeconds = null } = {}) {
  const parts = [`${name}=${value}`, `Path=${path}`, `SameSite=${sameSite}`];
  if (httpOnly) parts.push("HttpOnly");
  if (secure) parts.push("Secure");
  if (Number.isFinite(Number(maxAgeSeconds)) && Number(maxAgeSeconds) > 0) parts.push(`Max-Age=${Math.floor(Number(maxAgeSeconds))}`);
  return parts.join("; ");
}

function normalizeLimit(v, def) {
  const n = Number.parseInt(String(v || ""), 10);
  if (!Number.isFinite(n) || n <= 0) return def;
  return Math.min(n, 100);
}

async function readProjectKeyFromRuntimeConfig(projectRootAbs) {
  try {
    const p = resolve(projectRootAbs, "config", "PROJECT.json");
    const raw = await readFile(p, "utf8");
    const json = JSON.parse(raw);
    const projectKey = typeof json?.project_key === "string" ? json.project_key.trim() : "";
    if (projectKey) return projectKey;
    const projectCode = typeof json?.project_code === "string" ? json.project_code.trim() : "";
    if (projectCode) return projectCode;
  } catch {
    // ignore; fall back to directory name
  }
  return basename(projectRootAbs);
}

async function tailFile(pathAbs, lines = 50, maxBytes = 256 * 1024) {
  const p = resolve(pathAbs);
  const wantLines = Math.max(1, Number(lines) || 50);
  const max = Math.max(16 * 1024, Number(maxBytes) || 256 * 1024);
  try {
    const fh = await open(p, "r");
    try {
      const st = await fh.stat();
      let pos = st.size;
      let readBytes = 0;
      const chunk = 64 * 1024;
      let text = "";
      while (pos > 0 && readBytes < max) {
        const size = Math.min(chunk, pos, max - readBytes);
        pos -= size;
        const buf = Buffer.alloc(size);
        const r = await fh.read(buf, 0, size, pos);
        readBytes += r.bytesRead;
        text = buf.slice(0, r.bytesRead).toString("utf8") + text;
        const linesFound = text.split("\n").length - 1;
        if (linesFound >= wantLines + 2) break;
      }
      const arr = text.split("\n").filter((x) => x.length);
      const tail = arr.slice(-wantLines);
      return { ok: true, missing: false, text: tail.join("\n") + "\n" };
    } finally {
      await fh.close();
    }
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return { ok: false, missing: true, text: "" };
    return { ok: false, missing: false, text: "" };
  }
}

function isSameOrSubpath(a, b) {
  const rel = relative(a, b);
  if (!rel) return true;
  if (rel.startsWith("..")) return false;
  return !rel.includes("..") && !isAbsolute(rel);
}

async function tryReadInterviewState({ projectRoot, scope }) {
  const s = String(scope || "").trim();
  if (!s) return { ok: false, message: "Missing scope." };

  const scopeLower = s.toLowerCase();
  if (!(scopeLower === "system" || scopeLower.startsWith("repo:"))) return { ok: false, message: "Invalid scope. Expected: system | repo:<repo_id>." };

  let paths;
  try {
    paths = await loadProjectPaths({ projectRoot });
  } catch (err) {
    return { ok: false, message: err instanceof Error ? err.message : String(err) };
  }

  const repoId = scopeLower.startsWith("repo:") ? scopeLower.slice("repo:".length) : null;
  const mergedAbs = repoId ? resolve(paths.knowledge.ssotReposAbs, repoId, "assumptions.json") : resolve(paths.knowledge.ssotSystemAbs, "assumptions.json");

  const sessionsDirAbs = resolve(paths.knowledge.sessionsAbs);
  let latestSessionAbs = null;
  try {
    if (existsSync(sessionsDirAbs)) {
      const entries = await readdir(sessionsDirAbs, { withFileTypes: true });
      const files = entries
        .filter((e) => e.isFile() && e.name.startsWith("SESSION-") && e.name.endsWith(".md"))
        .map((e) => e.name)
        .sort((a, b) => a.localeCompare(b));
      if (files.length) latestSessionAbs = resolve(sessionsDirAbs, files[files.length - 1]);
    }
  } catch {
    latestSessionAbs = null;
  }

  const safeReadAbs = async (abs) => {
    if (!abs) return null;
    try {
      const t = await readFile(abs, "utf8");
      return String(t || "").slice(0, 4000);
    } catch {
      return null;
    }
  };

  const state = {
    scope: scopeLower === "system" ? "system" : `repo:${scopeLower.slice("repo:".length)}`,
    knowledge_repo_dir: paths.knowledge.rootAbs,
    latest_session_path: latestSessionAbs ? relative(paths.knowledge.rootAbs, latestSessionAbs) : null,
    merged_path: existsSync(mergedAbs) ? relative(paths.knowledge.rootAbs, mergedAbs) : null,
    session_excerpt: await safeReadAbs(latestSessionAbs),
    merged_excerpt: existsSync(mergedAbs) ? await safeReadAbs(mergedAbs) : null,
  };

  return { ok: true, exists: true, state };
}

function buildAuth({ passcodeHash, sessionSecret, cookieSecure }) {
  const sessions = new Map(); // sid -> { created_at, last_seen }
  const sessionMaxAgeSeconds = 12 * 60 * 60;

  const loginAttempts = new Map(); // ip -> { count, first_ms, locked_until_ms }
  const maxAttempts = 7;
  const windowMs = 10 * 60 * 1000;
  const lockMs = 15 * 60 * 1000;

  function cleanMaps() {
    const now = nowMs();
    for (const [sid, s] of sessions.entries()) {
      const last = typeof s?.last_seen_ms === "number" ? s.last_seen_ms : 0;
      if (now - last > sessionMaxAgeSeconds * 1000) sessions.delete(sid);
    }
    for (const [ip, a] of loginAttempts.entries()) {
      const first = typeof a?.first_ms === "number" ? a.first_ms : 0;
      const locked = typeof a?.locked_until_ms === "number" ? a.locked_until_ms : 0;
      if (locked && now > locked) loginAttempts.delete(ip);
      else if (!locked && now - first > windowMs) loginAttempts.delete(ip);
    }
  }

  function setSessionCookie(res, sid) {
    const sig = signSid(sessionSecret, sid);
    const value = `${sid}.${sig}`;
    res.setHeader("Set-Cookie", makeCookie({ name: "sid", value, secure: cookieSecure, maxAgeSeconds: sessionMaxAgeSeconds }));
  }

  function clearSessionCookie(res) {
    res.setHeader("Set-Cookie", makeCookie({ name: "sid", value: "deleted", secure: cookieSecure, maxAgeSeconds: 1 }));
  }

  function tryAuthorize(req) {
    cleanMaps();
    const cookies = parseCookies(req.headers.cookie || "");
    const raw = cookies.sid;
    if (!raw || !raw.includes(".")) return { ok: false, message: "Unauthorized" };
    const [sid, sig] = raw.split(".", 2);
    if (!sid || !sig) return { ok: false, message: "Unauthorized" };
    const expected = signSid(sessionSecret, sid);
    if (!safeEqualHex(sig, expected)) return { ok: false, message: "Unauthorized" };
    const s = sessions.get(sid);
    if (!s) return { ok: false, message: "Unauthorized" };
    s.last_seen_ms = nowMs();
    sessions.set(sid, s);
    req.session = { sid };
    return { ok: true, sid };
  }

  function authMiddleware(req, res, next) {
    const a = tryAuthorize(req);
    if (a.ok) return next();

    const path = typeof req.path === "string" ? req.path : "";
    const isApi = path.startsWith("/api/");
    if (!isApi && req.method === "GET") return res.redirect("/login");
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  async function loginHandler(req, res) {
    cleanMaps();
    const ip = String(req.headers["x-forwarded-for"] || req.socket.remoteAddress || "unknown").split(",")[0].trim();
    const now = nowMs();
    const cur = loginAttempts.get(ip) || { count: 0, first_ms: now, locked_until_ms: 0 };
    if (cur.locked_until_ms && now < cur.locked_until_ms) {
      const waitSec = Math.ceil((cur.locked_until_ms - now) / 1000);
      return res.status(429).json({ ok: false, message: `Too many attempts. Try again in ${waitSec}s.` });
    }
    if (now - cur.first_ms > windowMs) {
      cur.count = 0;
      cur.first_ms = now;
      cur.locked_until_ms = 0;
    }

    const passcode = typeof req.body?.passcode === "string" ? req.body.passcode : "";
    const ok = passcode ? bcrypt.compareSync(passcode, passcodeHash) : false;
    if (!ok) {
      cur.count += 1;
      if (cur.count >= maxAttempts) {
        cur.locked_until_ms = now + lockMs;
      }
      loginAttempts.set(ip, cur);
      return res.status(401).json({ ok: false, message: "Invalid passcode" });
    }

    // success
    loginAttempts.delete(ip);
    const sid = randomBytes(24).toString("hex");
    sessions.set(sid, { created_at: nowISO(), last_seen_ms: now });
    setSessionCookie(res, sid);
    return res.json({ ok: true });
  }

  function logoutHandler(req, res) {
    const sid = req?.session?.sid;
    if (sid) sessions.delete(sid);
    clearSessionCookie(res);
    res.json({ ok: true });
  }

  return { authMiddleware, loginHandler, logoutHandler, tryAuthorize };
}

async function main() {
  const WEB_PORT = Number.parseInt(readEnv("WEB_PORT", { fallback: "8090" }), 10);
  const WEB_BIND = readEnv("WEB_BIND", { fallback: "127.0.0.1" });
  const WEB_SESSION_SECRET = readEnv("WEB_SESSION_SECRET", { required: true });
  const WEB_PASSCODE_HASH = readEnv("WEB_PASSCODE_HASH", { required: true });
  const cookieSecure = readEnv("WEB_COOKIE_SECURE", { fallback: "" }) === "1";
  const cliTimeoutMs = Number.parseInt(readEnv("WEB_CLI_TIMEOUT_MS", { fallback: "120000" }), 10);

  const cfg = loadWebConfig();
  const engineRoot = resolve(cfg.AI_TEAM_REPO);
  if (isSameOrSubpath(engineRoot, cfg.OPS_ROOT)) {
    throw new Error("AI_PROJECT_ROOT must not be inside the AI-Team engine repo.");
  }

  // Ensure project ai/ exists or is creatable.
  await mkdir(resolve(cfg.OPS_ROOT, "ai"), { recursive: true });
  const projectKey = await readProjectKeyFromRuntimeConfig(cfg.OPS_ROOT);

  const app = express();
  app.set("trust proxy", true);

  // Minimal security headers
  app.use((req, res, next) => {
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("X-Frame-Options", "DENY");
    res.setHeader("Referrer-Policy", "no-referrer");
    res.setHeader("Cache-Control", "no-store");
    next();
  });

  app.use(express.json({ limit: "64kb" }));

  const auth = buildAuth({ passcodeHash: WEB_PASSCODE_HASH, sessionSecret: WEB_SESSION_SECRET, cookieSecure });

  const publicDir = resolve(engineRoot, "src", "web", "public");
  app.use("/static", express.static(publicDir, { fallthrough: false }));

  app.get("/", (req, res) => {
    if (auth.tryAuthorize(req).ok) return res.redirect("/lane-a");
    return res.redirect("/login");
  });

  app.get("/login", (req, res) => {
    if (auth.tryAuthorize(req).ok) return res.redirect("/lane-a");
    return res.sendFile(resolve(publicDir, "login.html"));
  });
  app.get("/lane-a", auth.authMiddleware, (req, res) => res.sendFile(resolve(publicDir, "lane-a.html")));
  app.get("/lane-a/skills", auth.authMiddleware, (_req, res) => res.redirect("/lane-a?tab=Skills"));
  app.get("/lane-b", auth.authMiddleware, (req, res) => res.sendFile(resolve(publicDir, "lane-b.html")));
  app.get("/bridge", auth.authMiddleware, (req, res) => res.sendFile(resolve(publicDir, "bridge.html")));
  app.get("/intake", auth.authMiddleware, (req, res) => res.sendFile(resolve(publicDir, "intake.html")));
  app.get("/interview", auth.authMiddleware, (req, res) => res.sendFile(resolve(publicDir, "interview.html")));
  app.get("/projects", auth.authMiddleware, (req, res) => res.sendFile(resolve(publicDir, "projects.html")));
  registerLaneAHealthRoutes(app, { engineRoot, authMiddleware: auth.authMiddleware });

  app.post("/api/login", auth.loginHandler);
  app.post("/api/logout", auth.authMiddleware, auth.logoutHandler);
  registerRunCommandRoutes(app, { authMiddleware: auth.authMiddleware, cliTimeoutMs });
  registerStatusOverviewRoutes(app, { engineRoot, authMiddleware: auth.authMiddleware, projectRootHint: cfg.OPS_ROOT });

  app.get("/api/ledger", auth.authMiddleware, async (req, res) => {
    const lines = normalizeLimit(req.query?.lines, 50);
    const ledgerPath = resolve(cfg.OPS_ROOT, "ai", "lane_b", "ledger.jsonl");
    const out = await tailFile(ledgerPath, lines);
    return res.json({ ok: out.ok, missing: out.missing, path: ledgerPath, text: out.text });
  });

  app.get("/api/project", auth.authMiddleware, async (req, res) => {
    return res.json({ ok: true, project_key: projectKey });
  });

  app.get("/api/projects", auth.authMiddleware, async (req, res) => {
    try {
      const regRes = await loadRegistry({ toolRepoRoot: engineRoot, createIfMissing: true });
      const projects = listProjects(regRes.registry);
      return res.json({ ok: true, host_id: regRes.registry.host_id, projects });
    } catch (err) {
      return res.status(500).json({ ok: false, message: err instanceof Error ? err.message : String(err) });
    }
  });

  app.get("/api/projects/:code", auth.authMiddleware, async (req, res) => {
    const code = typeof req.params?.code === "string" ? req.params.code.trim() : "";
    if (!code) return res.status(400).json({ ok: false, message: "Missing project code." });
    try {
      const regRes = await loadRegistry({ toolRepoRoot: engineRoot, createIfMissing: true });
      const project = getRegistryProject(regRes.registry, code);
      if (!project) return res.status(404).json({ ok: false, message: `Project not found: ${code}` });
      return res.json({ ok: true, project });
    } catch (err) {
      return res.status(500).json({ ok: false, message: err instanceof Error ? err.message : String(err) });
    }
  });

  app.get("/api/interview/state", auth.authMiddleware, async (req, res) => {
    const scope = typeof req.query?.scope === "string" ? req.query.scope.trim() : "";
    const state = await tryReadInterviewState({ projectRoot: cfg.OPS_ROOT, scope });
    return res.json(state);
  });

  app.listen(WEB_PORT, WEB_BIND, () => {
    // eslint-disable-next-line no-console
    console.log(`AI-Team Web UI listening on http://${WEB_BIND}:${WEB_PORT}`);
  });
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
