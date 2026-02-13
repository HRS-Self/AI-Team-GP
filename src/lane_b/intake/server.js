// .env loading is handled by the CLI entrypoint. Intake server should be configured via environment variables.
import http from "node:http";
import { enqueueInboxNote } from "./inbox.js";
import { runSweep } from "./sweep.js";
import { notifyIfNeeded } from "./notify.js";
import { readTextIfExists } from "../../utils/fs.js";

function parseBool(v) {
  const s = String(v || "").trim().toLowerCase();
  return s === "true" || s === "1" || s === "yes";
}

const port = Number.parseInt(process.env.INTAKE_PORT || "8787", 10);
const authToken = String(process.env.INTAKE_AUTH_TOKEN || "").trim();
const autoSweep = parseBool(process.env.INTAKE_AUTO_SWEEP || "false");

if (!authToken) {
  process.stderr.write("Missing INTAKE_AUTH_TOKEN. Set it in .env.\n");
  process.exit(2);
}

function isAuthorized(req) {
  const tokenHeader = req.headers["x-auth-token"];
  const auth = req.headers["authorization"];
  const bearer = typeof auth === "string" && auth.toLowerCase().startsWith("bearer ") ? auth.slice("bearer ".length).trim() : null;
  const provided = (typeof tokenHeader === "string" ? tokenHeader : null) || bearer || "";
  return !!provided && provided === authToken;
}

async function readJsonBody(req, maxBytes = 25 * 1024) {
  return await new Promise((resolve, reject) => {
    let size = 0;
    let body = "";
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        reject(new Error("Body too large"));
        req.destroy();
        return;
      }
      body += chunk.toString("utf8");
    });
    req.on("end", () => {
      if (!body) resolve({});
      else {
        try {
          resolve(JSON.parse(body));
        } catch {
          resolve({});
        }
      }
    });
    req.on("error", (err) => reject(err));
  });
}

function sendJson(res, code, obj) {
  res.statusCode = code;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(obj));
}

function sendText(res, code, text) {
  res.statusCode = code;
  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.end(String(text || ""));
}

function extractWhatsappText(body) {
  const b = body || {};
  if (typeof b.text === "string") return b.text;
  if (typeof b.body === "string") return b.body;
  if (typeof b.message === "string") return b.message;
  if (typeof b.message?.text === "string") return b.message.text;
  if (typeof b.data?.text === "string") return b.data.text;
  if (typeof b.payload?.text === "string") return b.payload.text;
  return null;
}

function extractWhatsappFrom(body) {
  const b = body || {};
  if (typeof b.from === "string") return b.from;
  if (typeof b.sender === "string") return b.sender;
  if (typeof b.message?.from === "string") return b.message.from;
  if (typeof b.data?.from === "string") return b.data.from;
  return null;
}

async function handleNote({ text, source }) {
  const t = String(text || "");
  if (!t.trim()) return { ok: false, status: 400, body: { ok: false, error: "Missing text" } };
  if (Buffer.byteLength(t, "utf8") > 20 * 1024) return { ok: false, status: 413, body: { ok: false, error: "Text too large (max 20KB)." } };

  const enq = await enqueueInboxNote({ text: t, source: source || "web" });

  let swept = false;
  let sweepResult = null;
  let notifyResult = null;
  if (autoSweep) {
    sweepResult = await runSweep();
    swept = sweepResult.ok;
    if (swept) notifyResult = await notifyIfNeeded();
  }

  return { ok: true, status: 200, body: { ok: true, intake_file: enq.intake_file, swept, sweep: sweepResult, notify: notifyResult } };
}

async function startExpressIfAvailable() {
  try {
    const [{ default: express }, { buildRoutes }] = await Promise.all([import("express"), import("./routes.js")]);
    const app = express();
    app.disable("x-powered-by");
    app.use(express.json({ limit: "25kb" }));
    app.get("/health", (_req, res) => res.json({ ok: true }));
    app.use(buildRoutes({ authToken, autoSweep }));
    const server = app.listen(port, () => {
      process.stdout.write(
        `Intake Assistant listening on http://127.0.0.1:${port} (express, auto_sweep=${autoSweep ? "true" : "false"})\n`,
      );
    });
    server.on("error", (err) => {
      process.stderr.write(`Intake Assistant failed to listen on 127.0.0.1:${port}: ${err instanceof Error ? err.message : String(err)}\n`);
      process.exit(1);
    });
    return true;
  } catch {
    return false;
  }
}

const usingExpress = await startExpressIfAvailable();
if (!usingExpress) {
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url || "/", "http://127.0.0.1");
    const method = String(req.method || "GET").toUpperCase();

    if (url.pathname === "/health" && method === "GET") {
      sendJson(res, 200, { ok: true, mode: "http-fallback" });
      return;
    }

    if (!isAuthorized(req)) {
      sendJson(res, 401, { ok: false, error: "Unauthorized" });
      return;
    }

    if (url.pathname === "/portfolio" && method === "GET") {
      const text = await readTextIfExists("ai/lane_b/PORTFOLIO.md");
      if (!text) {
        sendText(res, 404, "ai/lane_b/PORTFOLIO.md not found.");
        return;
      }
      sendText(res, 200, text);
      return;
    }

    if (url.pathname === "/decisions" && method === "GET") {
      const text = await readTextIfExists("ai/lane_b/DECISIONS_NEEDED.md");
      if (!text) {
        sendText(res, 404, "ai/lane_b/DECISIONS_NEEDED.md not found.");
        return;
      }
      sendText(res, 200, text);
      return;
    }

    if (url.pathname === "/note" && method === "POST") {
      let body;
      try {
        body = await readJsonBody(req);
      } catch {
        sendJson(res, 413, { ok: false, error: "Body too large" });
        return;
      }
      const result = await handleNote({ text: body?.text, source: body?.source || "web" });
      sendJson(res, result.status, result.body);
      return;
    }

    if (url.pathname === "/webhook/whatsapp" && method === "POST") {
      let body;
      try {
        body = await readJsonBody(req);
      } catch {
        sendJson(res, 413, { ok: false, error: "Body too large" });
        return;
      }

      const text = extractWhatsappText(body);
      const from = extractWhatsappFrom(body);
      const msg = text ? String(text).trim() : "";
      if (!msg) {
        sendJson(res, 400, { ok: false, error: "Missing message text" });
        return;
      }

      const prefix = from ? `[from:${from}] ` : "";
      const result = await handleNote({ text: `${prefix}${msg}`, source: "whatsapp" });
      sendJson(res, result.status, result.body);
      return;
    }

    sendJson(res, 404, { ok: false, error: "Not found" });
  });

  server.on("error", (err) => {
    process.stderr.write(`Intake Assistant failed to listen on 127.0.0.1:${port}: ${err instanceof Error ? err.message : String(err)}\n`);
    process.exit(1);
  });

  server.listen(port, "127.0.0.1", () => {
    process.stdout.write(
      `Intake Assistant listening on http://127.0.0.1:${port} (http-fallback, auto_sweep=${autoSweep ? "true" : "false"}; install express for full mode)\n`,
    );
  });
}
