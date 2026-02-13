import { readTextIfExists } from "../utils/fs.js";
import { loadRepoRegistry } from "../utils/repo-registry.js";

const POLICIES_PATH = "config/POLICIES.json";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

// Deep merge with array replace (no concat).
export function deepMerge(base, override) {
  if (Array.isArray(override)) return override.slice();
  if (!isPlainObject(override)) return override;
  const out = isPlainObject(base) ? { ...base } : {};
  for (const [k, v] of Object.entries(override)) {
    const bv = out[k];
    if (Array.isArray(v)) out[k] = v.slice();
    else if (isPlainObject(v)) out[k] = deepMerge(isPlainObject(bv) ? bv : {}, v);
    else out[k] = v;
  }
  return out;
}

export async function loadPolicies() {
  const text = await readTextIfExists(POLICIES_PATH);
  if (!text) return { ok: false, missing: true, message: `Missing ${POLICIES_PATH}.` };
  try {
    const parsed = JSON.parse(text);
    if (!parsed || parsed.version !== 1) return { ok: false, message: `Invalid ${POLICIES_PATH}: expected version=1.` };
    if (parsed.merge_strategy !== "deep_merge") return { ok: false, message: `Invalid ${POLICIES_PATH}: merge_strategy must be deep_merge.` };
    if (!Array.isArray(parsed.selectors)) return { ok: false, message: `Invalid ${POLICIES_PATH}: selectors[] missing.` };
    if (!isPlainObject(parsed.named)) return { ok: false, message: `Invalid ${POLICIES_PATH}: named map missing.` };
    return { ok: true, policies: parsed };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Invalid ${POLICIES_PATH}: JSON parse failed (${msg}).` };
  }
}

export function resolveEffectivePolicy({ repo, policies }) {
  const p = policies;
  const named = isPlainObject(p?.named) ? p.named : {};
  const selectors = Array.isArray(p?.selectors) ? p.selectors : [];

  let effective = {};
  const applied = [];

  for (const sel of selectors) {
    const match = isPlainObject(sel?.match) ? sel.match : {};
    let matches = true;
    for (const [k, v] of Object.entries(match)) {
      if (typeof repo?.[k] === "undefined") {
        matches = false;
        break;
      }
      if (repo[k] !== v) {
        matches = false;
        break;
      }
    }
    if (!matches) continue;

    const apply = Array.isArray(sel?.apply) ? sel.apply : [];
    for (const name of apply) {
      const blockName = String(name || "").trim();
      if (!blockName) continue;
      const block = named[blockName];
      if (!isPlainObject(block)) continue;
      effective = deepMerge(effective, block);
      applied.push(blockName);
    }
  }

  const overrides = repo?.PolicyOverrides;
  if (typeof overrides !== "undefined") {
    if (isPlainObject(overrides)) {
      effective = deepMerge(effective, overrides);
    }
  }

  return { effective, applied };
}

export async function policyShow({ repoId }) {
  const reposLoaded = await loadRepoRegistry();
  if (!reposLoaded.ok) return { ok: false, message: reposLoaded.message };

  const repo = (reposLoaded.registry.repos || []).find((r) => String(r?.repo_id || "").trim() === String(repoId || "").trim());
  if (!repo) return { ok: false, message: `Repo not found: ${repoId}` };

  const policiesLoaded = await loadPolicies();
  if (!policiesLoaded.ok) return { ok: false, message: policiesLoaded.message };

  const { effective, applied } = resolveEffectivePolicy({ repo, policies: policiesLoaded.policies });
  return {
    ok: true,
    repo: {
      repo_id: repo.repo_id,
      path: repo.path,
      status: repo.status,
      team_id: repo.team_id,
      IsHexa: repo.IsHexa,
      Usage: repo.Usage,
      Domains: repo.Domains,
    },
    applied,
    effective_policy: effective,
  };
}
