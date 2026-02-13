function stripDotenvLines(text) {
  const s = String(text || "");
  if (!s) return "";
  return s
    .split("\n")
    .filter((line) => !line.startsWith("[dotenv@"))
    .join("\n")
    .trim();
}

function safeJsonStringify(obj) {
  try {
    return JSON.stringify(obj, null, 2);
  } catch {
    return String(obj);
  }
}

function formatCliAction(action) {
  const a = action && typeof action === "object" ? action : {};
  const ok = a.ok === true;
  const exitCode = typeof a.exitCode === "number" ? a.exitCode : null;
  const timedOut = a.timedOut === true;

  const lines = [];
  lines.push(`Result: ${ok ? "OK" : "FAILED"}${exitCode !== null ? ` (exitCode=${exitCode})` : ""}${timedOut ? " (timedOut)" : ""}`);

  // Prefer structured JSON returned by the CLI (when available).
  const structured = a.result_json && typeof a.result_json === "object" ? a.result_json : null;
  if (structured) {
    if (typeof structured.message === "string" && structured.message.trim()) {
      lines.push("");
      lines.push(structured.message.trim());
    }
    lines.push("");
    lines.push("Details:");
    lines.push(safeJsonStringify(structured));
    return lines.join("\n");
  }

  // Otherwise show stderr (usually already formatted), then stdout without dotenv noise.
  const stderr = stripDotenvLines(a.stderr);
  const stdout = stripDotenvLines(a.stdout);
  const body = (stderr && stderr.trim()) || (stdout && stdout.trim()) || "";
  if (body) {
    lines.push("");
    lines.push(body.trim());
  }

  // Keep raw output available (still text) for debugging.
  const raw = [];
  if (stderr) raw.push(`--- stderr ---\n${stderr}`);
  if (stdout) raw.push(`--- stdout ---\n${stdout}`);
  if (raw.length) {
    lines.push("");
    lines.push("Raw:");
    lines.push(raw.join("\n\n"));
  }

  return lines.join("\n");
}

function formatKnowledgeStatus(status) {
  const st = status && typeof status === "object" ? status : {};
  const lines = [];
  if (typeof st.overall === "string") lines.push(`overall: ${st.overall}`);
  if (typeof st.generated_at === "string") lines.push(`generated_at: ${st.generated_at}`);
  if (typeof st.projectRoot === "string") lines.push(`ops_root: ${st.projectRoot}`);
  if (typeof st.knowledge_repo === "string") lines.push(`knowledge_repo: ${st.knowledge_repo}`);
  if (typeof st.repos_root === "string") lines.push(`repos_root: ${st.repos_root}`);

  if (st.system && typeof st.system === "object") {
    const sys = st.system;
    lines.push("");
    lines.push("system:");
    lines.push(`- scan_complete_all_repos: ${sys.scan_complete_all_repos === true ? "true" : "false"}`);
    lines.push(`- open_decisions_count: ${typeof sys.open_decisions_count === "number" ? sys.open_decisions_count : 0}`);
    lines.push(`- integration_gaps_unresolved_count: ${typeof sys.integration_gaps_unresolved_count === "number" ? sys.integration_gaps_unresolved_count : 0}`);
    const backed = typeof sys.evidence?.backed_facts === "number" ? sys.evidence.backed_facts : 0;
    const orphans = typeof sys.evidence?.orphan_claims === "number" ? sys.evidence.orphan_claims : 0;
    lines.push(`- evidence: backed_facts=${backed} orphan_claims=${orphans}`);
  }

  if (Array.isArray(st.repos)) {
    lines.push("");
    lines.push("repos:");
    for (const r of st.repos) {
      if (!r || typeof r !== "object") continue;
      const id = typeof r.repo_id === "string" ? r.repo_id : "(unknown)";
      const scan = r.scan && typeof r.scan === "object" ? r.scan : {};
      const freshness = r.freshness && typeof r.freshness === "object" ? r.freshness : {};
      const evidence = r.evidence && typeof r.evidence === "object" ? r.evidence : {};
      const dec = r.decisions && typeof r.decisions === "object" ? r.decisions : {};

      const complete = scan.complete === true ? "complete" : "incomplete";
      const scanAt = typeof scan.last_scan_at === "string" ? scan.last_scan_at : "null";
      const failures = typeof scan.failures === "number" ? scan.failures : 0;
      const stale = freshness.stale === true ? "stale" : "fresh";
      const staleReason = typeof freshness.stale_reason === "string" && freshness.stale_reason ? ` (${freshness.stale_reason})` : "";
      const backed = typeof evidence.backed_facts === "number" ? evidence.backed_facts : 0;
      const orphans = typeof evidence.orphan_claims === "number" ? evidence.orphan_claims : 0;
      const openDec = typeof dec.open_count === "number" ? dec.open_count : 0;

      lines.push(`- ${id}: scan=${complete} at=${scanAt} failures=${failures} ${stale}${staleReason} backed=${backed} orphan=${orphans} open_decisions=${openDec}`);
    }
  }

  return lines.join("\n").trim() || safeJsonStringify(st);
}

let registryPromise = null;
let actionMapPromise = null;

async function fetchCommandRegistry() {
  if (!registryPromise) {
    registryPromise = fetch("/api/command-registry?webOnly=1", {
      method: "GET",
      credentials: "include",
    }).then(async (res) => {
      const json = await res.json().catch(() => ({}));
      if (!res.ok || !json || json.ok !== true || !Array.isArray(json.commands)) {
        throw new Error("Failed to load command registry.");
      }
      return json.commands;
    });
  }
  return registryPromise;
}

async function fetchCommandActionMap() {
  if (!actionMapPromise) {
    actionMapPromise = fetchCommandRegistry().then((commands) => {
      const map = new Map();
      for (const item of commands) {
        const action = item && typeof item.webAction === "string" ? item.webAction.trim() : "";
        if (action) map.set(action, item.cmd);
      }
      return map;
    });
  }
  return actionMapPromise;
}

async function runWebCommand(action, args = {}, { stream = false, onEvent = null } = {}) {
  const actions = await fetchCommandActionMap();
  const cmd = actions.get(String(action || "").trim());
  if (!cmd) {
    return { status: 400, ok: false, json: { ok: false, message: `Unknown web action: ${action}` } };
  }

  if (!stream) {
    const res = await fetch("/api/run-command", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ cmd, args }),
    });
    const json = await res.json().catch(() => ({}));
    return { status: res.status, ok: res.ok, json };
  }

  const res = await fetch("/api/run-command?stream=1", {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
    credentials: "include",
    body: JSON.stringify({ cmd, args, stream: true }),
  });

  const decoder = new TextDecoder();
  const reader = res.body?.getReader ? res.body.getReader() : null;
  if (!reader) return { status: res.status, ok: res.ok, json: { ok: false, message: "Streaming unavailable." } };

  let event = "message";
  let data = "";
  let donePayload = null;
  let buffer = "";

  // eslint-disable-next-line no-constant-condition
  while (true) {
    // eslint-disable-next-line no-await-in-loop
    const chunk = await reader.read();
    if (chunk.done) break;
    buffer += decoder.decode(chunk.value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";
    for (const line of lines) {
      if (!line.trim()) {
        let payload = {};
        if (data) {
          try {
            payload = JSON.parse(data);
          } catch {
            payload = { raw: data };
          }
        }
        if (typeof onEvent === "function") onEvent({ event, data: payload });
        if (event === "done") donePayload = payload;
        event = "message";
        data = "";
        continue;
      }
      if (line.startsWith("event:")) {
        event = line.slice("event:".length).trim();
      } else if (line.startsWith("data:")) {
        data = line.slice("data:".length).trim();
      }
    }
  }

  return { status: res.status, ok: res.ok, json: donePayload || { ok: false, message: "Stream ended without done payload." } };
}

window.AI_TEAM_UI = {
  formatCliAction,
  formatKnowledgeStatus,
  fetchCommandRegistry,
  runWebCommand,
};
