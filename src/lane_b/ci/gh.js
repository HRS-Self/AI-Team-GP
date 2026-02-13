import { spawnSync } from "node:child_process";

function runGh(args, { cwd = null, timeoutMs = null } = {}) {
  const argv = Array.isArray(args) ? args.map((a) => String(a)) : [];
  const cmd = `gh ${argv
    .map((a) => {
      if (!a) return '""';
      if (/[\s"]/u.test(a)) return JSON.stringify(a);
      return a;
    })
    .join(" ")}`.trim();

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

export function requireGhOrDie() {
  const missingMsg = "Missing required dependency: gh (GitHub CLI). Install it: https://cli.github.com/";
  const authMsg = "GitHub CLI (gh) is required. Install it and run `gh auth login`.";

  const version = runGh(["--version"]);
  if (!version.ok) return { ok: false, message: missingMsg, details: { step: "gh --version", ...version } };

  const auth = runGh(["auth", "status", "-h", "github.com"]);
  if (!auth.ok) return { ok: false, message: authMsg, details: { step: "gh auth status -h github.com", ...auth } };

  return { ok: true };
}

function parseJsonOrNull(text) {
  try {
    return JSON.parse(String(text || ""));
  } catch {
    return null;
  }
}

export function ghPrList({ cwd, base, head }) {
  const res = runGh(["pr", "list", "--state", "open", "--base", String(base), "--head", String(head), "--json", "number,url", "--limit", "1"], { cwd });
  if (!res.ok) return { ok: false, ...res };
  const parsed = parseJsonOrNull(res.stdout);
  const first = Array.isArray(parsed) && parsed.length ? parsed[0] : null;
  return { ok: true, number: typeof first?.number === "number" ? first.number : null, url: typeof first?.url === "string" ? first.url : null, raw: parsed };
}

export function ghPrView({ cwd, pr }) {
  const res = runGh(["pr", "view", String(pr), "--json", "number,url,baseRefName,headRefName,headRefOid,state"], { cwd });
  if (!res.ok) return { ok: false, ...res };
  const parsed = parseJsonOrNull(res.stdout);
  if (!parsed) return { ok: false, status: res.status, stdout: res.stdout, stderr: res.stderr, error: "invalid_json", cmd: res.cmd };
  return {
    ok: true,
    number: typeof parsed?.number === "number" ? parsed.number : null,
    url: typeof parsed?.url === "string" ? parsed.url : null,
    base: typeof parsed?.baseRefName === "string" ? parsed.baseRefName : null,
    head: typeof parsed?.headRefName === "string" ? parsed.headRefName : null,
    head_sha: typeof parsed?.headRefOid === "string" ? parsed.headRefOid : null,
    state: typeof parsed?.state === "string" ? parsed.state : null,
    raw: parsed,
  };
}

export function ghPrCreate({ cwd, base, head, title, body }) {
  const res = runGh(
    [
      "pr",
      "create",
      "--base",
      String(base),
      "--head",
      String(head),
      "--title",
      String(title),
      "--body",
      String(body),
    ],
    { cwd },
  );
  if (!res.ok) return { ok: false, ...res };
  const url = res.stdout
    .trim()
    .split("\n")
    .map((l) => l.trim())
    .find((l) => /^https?:\/\//i.test(l));
  return { ok: true, url: url || null, stdout: res.stdout, stderr: res.stderr };
}

export function ghPrChecksJson({ cwd, prNumber }) {
  const res = runGh(["pr", "checks", String(prNumber), "--json", "bucket,completedAt,description,event,link,name,startedAt,state,workflow"], { cwd });
  if (!res.ok) return { ok: false, ...res };
  const parsed = parseJsonOrNull(res.stdout);
  return { ok: true, checks: Array.isArray(parsed) ? parsed : [], raw: parsed };
}

export function ghPrChecksWatch({ cwd, prNumber, timeoutMs = null }) {
  // Uses gh's built-in watch mode (required); we still call ghPrChecksJson afterwards for structured state.
  return runGh(["pr", "checks", String(prNumber), "--watch", "--fail-fast"], { cwd, timeoutMs });
}

export function ghRunListByBranch({ cwd, branch, limit = 20 }) {
  const res = runGh(
    ["run", "list", "--branch", String(branch), "--json", "databaseId,htmlUrl,status,conclusion,event,workflowName,headSha,createdAt", "--limit", String(limit)],
    { cwd },
  );
  if (!res.ok) return { ok: false, ...res };
  const parsed = parseJsonOrNull(res.stdout);
  return { ok: true, runs: Array.isArray(parsed) ? parsed : [], raw: parsed };
}

export function ghRunViewLogFailed({ cwd, runId, timeoutMs = null }) {
  return runGh(["run", "view", String(runId), "--log-failed"], { cwd, timeoutMs });
}
