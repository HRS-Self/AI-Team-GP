import express from "express";
import { readTextIfExists } from "../../utils/fs.js";
import { enqueueInboxNote } from "./inbox.js";
import { runSweep } from "./sweep.js";
import { notifyIfNeeded } from "./notify.js";

function authMiddleware({ authToken }) {
  return function auth(req, res, next) {
    const tokenHeader = req.header("x-auth-token");
    const auth = req.header("authorization");
    const bearer = auth && auth.toLowerCase().startsWith("bearer ") ? auth.slice("bearer ".length).trim() : null;

    const provided = tokenHeader || bearer || "";
    if (!provided || provided !== authToken) {
      res.status(401).json({ ok: false, error: "Unauthorized" });
      return;
    }
    next();
  };
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

export function buildRoutes({ authToken, autoSweep }) {
  const router = express.Router();

  router.use(authMiddleware({ authToken }));

  router.post("/note", async (req, res) => {
    const text = typeof req.body?.text === "string" ? req.body.text : "";
    const source = typeof req.body?.source === "string" ? req.body.source : "web";

    if (!text.trim()) {
      res.status(400).json({ ok: false, error: "Missing text" });
      return;
    }

    if (Buffer.byteLength(text, "utf8") > 20 * 1024) {
      res.status(413).json({ ok: false, error: "Text too large (max 20KB)." });
      return;
    }

    const enq = await enqueueInboxNote({ text, source });

    let swept = false;
    let sweepResult = null;
    let notifyResult = null;

    if (autoSweep) {
      sweepResult = await runSweep();
      swept = sweepResult.ok;
      if (swept) {
        notifyResult = await notifyIfNeeded();
      }
    }

    res.json({ ok: true, intake_file: enq.intake_file, swept, sweep: sweepResult, notify: notifyResult });
  });

  router.get("/portfolio", async (_req, res) => {
    const text = await readTextIfExists("ai/lane_b/PORTFOLIO.md");
    if (!text) {
      res.status(404).type("text/plain").send("ai/lane_b/PORTFOLIO.md not found.");
      return;
    }
    res.type("text/plain").send(text);
  });

  router.get("/decisions", async (_req, res) => {
    const text = await readTextIfExists("ai/lane_b/DECISIONS_NEEDED.md");
    if (!text) {
      res.status(404).type("text/plain").send("ai/lane_b/DECISIONS_NEEDED.md not found.");
      return;
    }
    res.type("text/plain").send(text);
  });

  router.post("/webhook/whatsapp", async (req, res) => {
    const text = extractWhatsappText(req.body);
    const from = extractWhatsappFrom(req.body);
    const msg = text ? text.trim() : "";
    if (!msg) {
      res.status(400).json({ ok: false, error: "Missing message text" });
      return;
    }

    if (Buffer.byteLength(msg, "utf8") > 20 * 1024) {
      res.status(413).json({ ok: false, error: "Text too large (max 20KB)." });
      return;
    }

    const prefix = from ? `[from:${from}] ` : "";
    const enq = await enqueueInboxNote({ text: `${prefix}${msg}`, source: "whatsapp" });

    let swept = false;
    let sweepResult = null;
    let notifyResult = null;

    if (autoSweep) {
      sweepResult = await runSweep();
      swept = sweepResult.ok;
      if (swept) {
        notifyResult = await notifyIfNeeded();
      }
    }

    res.json({ ok: true, intake_file: enq.intake_file, swept, sweep: sweepResult, notify: notifyResult });
  });

  return router;
}
