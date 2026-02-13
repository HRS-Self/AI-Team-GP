import { spawnSync } from "node:child_process";

function runGh(args, { cwd = null, timeoutMs = null } = {}) {
  const argv = Array.isArray(args) ? args.map((a) => String(a)) : [];
  const res = spawnSync("gh", argv, {
    cwd: cwd || undefined,
    shell: false,
    encoding: "utf8",
    timeout: Number.isFinite(Number(timeoutMs)) && Number(timeoutMs) > 0 ? Number(timeoutMs) : undefined,
    env: { ...process.env, GH_PAGER: "cat", PAGER: "cat" },
  });

  const errMsg = res.error instanceof Error ? res.error.message : res.error ? String(res.error) : null;
  const errorCode =
    res.error && typeof res.error === "object" && "code" in res.error && typeof res.error.code === "string" ? res.error.code : null;

  const cmd = `gh ${argv
    .map((a) => {
      if (!a) return '""';
      if (/[\s"]/u.test(a)) return JSON.stringify(a);
      return a;
    })
    .join(" ")}`.trim();

  return {
    ok: res.status === 0,
    status: typeof res.status === "number" ? res.status : null,
    signal: res.signal || null,
    stdout: String(res.stdout || ""),
    stderr: String(res.stderr || ""),
    error: errMsg,
    error_code: errorCode,
    timed_out: String(errorCode || "").toUpperCase() === "ETIMEDOUT",
    cmd,
  };
}

export function assertGhInstalled() {
  const missingMsg = "Missing required dependency: gh (GitHub CLI). Install it: https://cli.github.com/";
  const version = runGh(["--version"]);
  if (!version.ok) {
    const err = new Error(missingMsg);
    err.details = { step: "gh --version", ...version };
    throw err;
  }
}

export function assertGhAuthenticated() {
  const authMsg = "GitHub CLI (gh) is required. Install it and run `gh auth login`.";
  const auth = runGh(["auth", "status", "-h", "github.com"]);
  if (!auth.ok) {
    const err = new Error(authMsg);
    err.details = { step: "gh auth status -h github.com", ...auth };
    throw err;
  }
}

export function assertGhReady() {
  assertGhInstalled();
  assertGhAuthenticated();
}

function parseJsonOrThrow(text, label) {
  try {
    return JSON.parse(String(text || ""));
  } catch {
    const err = new Error(`gh returned invalid JSON for ${label}.`);
    err.details = { stdout: String(text || "").slice(0, 2000) };
    throw err;
  }
}

export function ghJson(args, { cwd = null, timeoutMs = null, label = "ghJson" } = {}) {
  const res = runGh(args, { cwd, timeoutMs });
  if (!res.ok) {
    const err = new Error(`gh failed: ${label}`);
    err.details = res;
    throw err;
  }
  return parseJsonOrThrow(res.stdout, label);
}

export function addPrLabel({ repo, prNumber, labels }) {
  const r = String(repo || "").trim();
  const pr = String(prNumber || "").trim();
  const ls = Array.isArray(labels) ? labels.map((x) => String(x || "").trim()).filter(Boolean) : [];
  if (!r) throw new Error("addPrLabel: missing repo (owner/name).");
  if (!pr) throw new Error("addPrLabel: missing prNumber.");
  if (!ls.length) return { ok: true, skipped: true };

  const args = ["pr", "edit", pr, "--repo", r];
  for (const l of ls) args.push("--add-label", l);
  const res = runGh(args);
  if (!res.ok) {
    const err = new Error("Failed to add PR label(s).");
    err.details = res;
    throw err;
  }
  return { ok: true };
}

export function createPr({ repo, base, head, title, body, labels = [] }) {
  const r = String(repo || "").trim();
  if (!r) throw new Error("createPr: missing repo (owner/name).");
  const res = runGh(["pr", "create", "--repo", r, "--base", String(base), "--head", String(head), "--title", String(title), "--body", String(body)]);
  if (!res.ok) {
    const err = new Error("Failed to create PR.");
    err.details = res;
    throw err;
  }
  const url = res.stdout
    .trim()
    .split("\n")
    .map((l) => l.trim())
    .find((l) => /^https?:\/\//i.test(l));
  if (!url) {
    const err = new Error("Failed to parse PR URL from gh output.");
    err.details = res;
    throw err;
  }
  if (labels.length) addPrLabel({ repo: r, prNumber: url, labels }); // gh accepts URL in `pr edit`.
  return { ok: true, url };
}

export function getPrChecks({ repo, prNumber }) {
  const r = String(repo || "").trim();
  const pr = String(prNumber || "").trim();
  if (!r) throw new Error("getPrChecks: missing repo (owner/name).");
  if (!pr) throw new Error("getPrChecks: missing prNumber.");
  const json = ghJson(["pr", "view", pr, "--repo", r, "--json", "statusCheckRollup,number,url,baseRefName,headRefName,headRefOid,state"], { label: "gh pr view --json statusCheckRollup" });
  return { ok: true, pr: json, checks: Array.isArray(json?.statusCheckRollup) ? json.statusCheckRollup : [] };
}

export function prNumberFromUrl(url) {
  const u = String(url || "").trim();
  const m = u.match(/\/pull\/(\d+)(?:\/|$)/i);
  return m ? Number.parseInt(m[1], 10) : null;
}

