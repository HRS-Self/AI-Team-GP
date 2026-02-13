function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
}

export function parseGitHubOwnerRepo(remoteUrl) {
  const raw = String(remoteUrl || "").trim();
  if (!raw) return null;

  // https://github.com/Owner/Repo(.git)
  let m = raw.match(/^https?:\/\/github\.com\/([^/]+)\/([^/]+?)(?:\.git)?\/?$/i);
  if (m) return { owner: m[1], repo: m[2] };

  // git@github.com:Owner/Repo(.git)
  m = raw.match(/^git@github\.com:([^/]+)\/([^/]+?)(?:\.git)?$/i);
  if (m) return { owner: m[1], repo: m[2] };

  // ssh://git@github.com/Owner/Repo(.git)
  m = raw.match(/^ssh:\/\/git@github\.com\/([^/]+)\/([^/]+?)(?:\.git)?\/?$/i);
  if (m) return { owner: m[1], repo: m[2] };

  return null;
}

async function ghFetchJson(url, { token }) {
  const res = await fetch(url, {
    method: "GET",
    headers: {
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      ...(isNonEmptyString(token) ? { Authorization: `Bearer ${token.trim()}` } : {}),
    },
  });

  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }

  return { ok: res.ok, status: res.status, json, text };
}

export async function waitForGitHubActionsRun({
  owner,
  repo,
  token,
  branch,
  headSha,
  timeoutMs = 20 * 60_000,
  pollIntervalMs = 15_000,
} = {}) {
  if (!isNonEmptyString(owner) || !isNonEmptyString(repo)) return { ok: false, error: "missing_owner_repo" };
  if (!isNonEmptyString(branch)) return { ok: false, error: "missing_branch" };
  if (!isNonEmptyString(headSha)) return { ok: false, error: "missing_head_sha" };

  const startedAt = Date.now();
  const deadline = startedAt + Math.max(5_000, Number(timeoutMs) || 0);
  const pollMs = Math.max(2_000, Number(pollIntervalMs) || 0);

  let last = null;
  while (Date.now() < deadline) {
    const url = `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/actions/runs?branch=${encodeURIComponent(
      branch,
    )}&per_page=20`;

    const res = await ghFetchJson(url, { token });
    last = res;
    if (!res.ok) {
      return {
        ok: false,
        error: "github_api_error",
        status: res.status,
        message: typeof res.json?.message === "string" ? res.json.message : res.text?.slice(0, 200) || "GitHub API error",
      };
    }

    const runs = Array.isArray(res.json?.workflow_runs) ? res.json.workflow_runs : [];
    const matching = runs.find((r) => String(r?.head_sha || "") === String(headSha));
    if (matching) {
      const status = String(matching.status || "");
      const conclusion = matching.conclusion === null ? null : String(matching.conclusion || "");
      const html_url = typeof matching.html_url === "string" ? matching.html_url : null;

      if (status === "completed") {
        return {
          ok: true,
          state: "completed",
          conclusion,
          status,
          html_url,
          run_id: matching.id ?? null,
          waited_ms: Date.now() - startedAt,
        };
      }

      // Found but not complete yet; keep polling.
    }

    await new Promise((r) => setTimeout(r, pollMs));
  }

  return {
    ok: false,
    error: "timeout",
    waited_ms: Date.now() - startedAt,
    last_status: last?.status ?? null,
  };
}

