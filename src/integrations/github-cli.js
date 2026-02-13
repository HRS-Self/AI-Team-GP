import { spawnSync } from "node:child_process";

function nowISO() {
  return new Date().toISOString();
}

function run(cmd, { cwd = null, timeoutMs = null } = {}) {
  const res = spawnSync(cmd, {
    cwd: cwd || undefined,
    shell: true,
    encoding: "utf8",
    timeout: Number.isFinite(Number(timeoutMs)) && Number(timeoutMs) > 0 ? Number(timeoutMs) : undefined,
    env: { ...process.env, GH_PAGER: "cat", PAGER: "cat" },
  });
  const errMsg = res.error instanceof Error ? res.error.message : res.error ? String(res.error) : null;
  return {
    ok: res.status === 0,
    status: typeof res.status === "number" ? res.status : null,
    signal: res.signal || null,
    stdout: String(res.stdout || ""),
    stderr: String(res.stderr || ""),
    error: errMsg,
    timed_out: !!(res.error && String(res.error.code || "").toUpperCase() === "ETIMEDOUT"),
  };
}

export function ensureGhAvailableAndAuthed() {
  const requiredMessage = "GitHub CLI (gh) is required. Install it and run `gh auth login`.";

  const version = run("gh --version");
  if (!version.ok) {
    return {
      ok: false,
      message: requiredMessage,
      details: { step: "gh --version", ...version },
    };
  }

  const auth = run("gh auth status -h github.com");
  if (!auth.ok) {
    return {
      ok: false,
      message: requiredMessage,
      details: { step: "gh auth status", ...auth },
    };
  }

  return { ok: true };
}

export function ghPrList({ cwd, base, head }) {
  const cmd = `gh pr list --state open --base "${String(base).replaceAll('"', '\\"')}" --head "${String(head).replaceAll('"', '\\"')}" --json number,url --limit 1`;
  const res = run(cmd, { cwd });
  if (!res.ok) return { ok: false, ...res };
  let parsed = null;
  try {
    parsed = JSON.parse(res.stdout);
  } catch {
    parsed = null;
  }
  const first = Array.isArray(parsed) && parsed.length ? parsed[0] : null;
  const number = first && typeof first.number === "number" ? first.number : null;
  const url = first && typeof first.url === "string" ? first.url : null;
  return { ok: true, number, url, raw: parsed };
}

export function ghPrCreate({ cwd, base, head, title, body }) {
  const cmd =
    `gh pr create --base "${String(base).replaceAll('"', '\\"')}"` +
    ` --head "${String(head).replaceAll('"', '\\"')}"` +
    ` --title "${String(title).replaceAll('"', '\\"')}"` +
    ` --body "${String(body).replaceAll('"', '\\"')}"`;
  const res = run(cmd, { cwd });
  if (!res.ok) return { ok: false, ...res };
  const url = res.stdout.trim().split("\n").map((l) => l.trim()).filter(Boolean).find((l) => /^https?:\/\//i.test(l)) || null;
  return { ok: true, url, stdout: res.stdout, stderr: res.stderr };
}

export function ghPrView({ cwd, head }) {
  const cmd = `gh pr view "${String(head).replaceAll('"', '\\"')}" --json number,url,baseRefName,headRefName,state`;
  const res = run(cmd, { cwd });
  if (!res.ok) return { ok: false, ...res };
  try {
    const parsed = JSON.parse(res.stdout);
    return {
      ok: true,
      number: typeof parsed?.number === "number" ? parsed.number : null,
      url: typeof parsed?.url === "string" ? parsed.url : null,
      base: typeof parsed?.baseRefName === "string" ? parsed.baseRefName : null,
      head: typeof parsed?.headRefName === "string" ? parsed.headRefName : null,
      state: typeof parsed?.state === "string" ? parsed.state : null,
      raw: parsed,
    };
  } catch {
    return { ok: false, status: res.status, stdout: res.stdout, stderr: res.stderr, error: "invalid_json" };
  }
}

export function ghPrChecksWatch({ cwd, prNumber }) {
  const cmd = `gh pr checks ${Number(prNumber)} --watch`;
  const started_at = nowISO();
  const res = run(cmd, { cwd });
  return { ...res, started_at, finished_at: nowISO() };
}

