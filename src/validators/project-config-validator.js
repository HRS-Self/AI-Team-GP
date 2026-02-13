function isPlainObject(x) {
      return !!x && typeof x === 'object' && !Array.isArray(x);
}

function isNonEmptyString(x) {
      return typeof x === 'string' && x.trim().length > 0;
}

function isAbsolutePosixPath(s) {
      return typeof s === 'string' && s.trim().startsWith('/');
}

function hasBasename(absPath, baseName) {
      const s = typeof absPath === 'string' ? absPath.trim() : '';
      const bn = typeof baseName === 'string' ? baseName.trim() : '';
      if (!s || !bn) return false;
      const norm = s.endsWith('/') ? s.slice(0, -1) : s;
      return norm.endsWith(`/${bn}`);
}

function normalizeStringArray(arr) {
      if (!Array.isArray(arr)) return null;
      const out = [];
      for (const v of arr) {
            const s = String(v || '').trim();
            if (!s) continue;
            if (!out.includes(s)) out.push(s);
      }
      return out;
}

function deprecatedToken() {
      return ['p', 'r', 'o', 'g', 'r', 'a', 'm'].join('');
}

function findDeprecatedKey(obj) {
      if (!isPlainObject(obj)) return null;
      const needle = deprecatedToken();
      for (const k of Object.keys(obj)) {
            if (
                  String(k || '')
                        .toLowerCase()
                        .includes(needle)
            )
                  return String(k);
      }
      return null;
}

export function validateProjectConfig(raw) {
      const errors = [];
      const add = (m) => errors.push(String(m));

      if (!isPlainObject(raw))
            return {
                  ok: false,
                  errors: ['config/PROJECT.json must be a JSON object.'],
                  normalized: null,
            };

      const allowedKeys = new Set([
            'version',
            'project_code',
            'ops_root_abs',
            'repos_root_abs',
            'knowledge_repo_dir',
            'ssot_bundle_policy',
      ]);
      for (const k of Object.keys(raw)) {
            if (!allowedKeys.has(k))
                  add(`config/PROJECT.json contains unknown key '${k}'.`);
      }

      const badKey = findDeprecatedKey(raw);
      if (badKey)
            add(
                  `config/PROJECT.json contains deprecated key '${badKey}'. Use repo + team scoping only.`,
            );

      const version = raw.version;
      if (version !== 4) add('config/PROJECT.json.version must be 4.');

      const project_code = isNonEmptyString(raw.project_code)
            ? raw.project_code.trim()
            : null;
      if (!project_code)
            add('config/PROJECT.json.project_code must be a non-empty string.');

      const ops_root_abs = isNonEmptyString(raw.ops_root_abs)
            ? raw.ops_root_abs.trim()
            : null;
      if (!ops_root_abs)
            add(
                  'config/PROJECT.json.ops_root_abs must be a non-empty absolute path to OPS_ROOT (e.g., /opt/AI-Projects/<code>/ops).',
            );
      if (ops_root_abs && !isAbsolutePosixPath(ops_root_abs))
            add(
                  'config/PROJECT.json.ops_root_abs must be an absolute POSIX path.',
            );
      if (
            ops_root_abs &&
            isAbsolutePosixPath(ops_root_abs) &&
            !hasBasename(ops_root_abs, 'ops')
      )
            add("config/PROJECT.json.ops_root_abs must end with '/ops'.");

      const repos_root_abs = isNonEmptyString(raw.repos_root_abs)
            ? raw.repos_root_abs.trim()
            : null;
      if (!repos_root_abs)
            add(
                  'config/PROJECT.json.repos_root_abs must be a non-empty absolute path to REPOS_ROOT (e.g., /opt/AI-Projects/<code>/repos).',
            );
      if (repos_root_abs && !isAbsolutePosixPath(repos_root_abs))
            add(
                  'config/PROJECT.json.repos_root_abs must be an absolute POSIX path.',
            );
      if (
            repos_root_abs &&
            isAbsolutePosixPath(repos_root_abs) &&
            !hasBasename(repos_root_abs, 'repos')
      )
            add("config/PROJECT.json.repos_root_abs must end with '/repos'.");

      const knowledge_repo_dir = isNonEmptyString(raw.knowledge_repo_dir)
            ? raw.knowledge_repo_dir.trim()
            : null;
      if (!knowledge_repo_dir)
            add(
                  'config/PROJECT.json.knowledge_repo_dir must be a non-empty absolute path to the knowledge git repo root (K_ROOT).',
            );
      if (knowledge_repo_dir && !isAbsolutePosixPath(knowledge_repo_dir))
            add(
                  'config/PROJECT.json.knowledge_repo_dir must be an absolute POSIX path.',
            );
      if (
            knowledge_repo_dir &&
            isAbsolutePosixPath(knowledge_repo_dir) &&
            !hasBasename(knowledge_repo_dir, 'knowledge')
      )
            add(
                  "config/PROJECT.json.knowledge_repo_dir must end with '/knowledge'.",
            );

      const ssot_bundle_policy =
            raw.ssot_bundle_policy === null ||
            typeof raw.ssot_bundle_policy === 'undefined'
                  ? null
                  : raw.ssot_bundle_policy;
      let globalPacks = null;
      if (ssot_bundle_policy !== null) {
            if (!isPlainObject(ssot_bundle_policy))
                  add(
                        'config/PROJECT.json.ssot_bundle_policy must be an object (or null/omit).',
                  );
            globalPacks = normalizeStringArray(
                  ssot_bundle_policy?.global_packs,
            );
            if (ssot_bundle_policy && globalPacks === null)
                  add(
                        'config/PROJECT.json.ssot_bundle_policy.global_packs must be a string[] (use [] for none).',
                  );
      }

      if (errors.length) return { ok: false, errors, normalized: null };
      return {
            ok: true,
            errors: [],
            normalized: {
                  version: 4,
                  project_code,
                  ops_root_abs,
                  repos_root_abs,
                  knowledge_repo_dir,
                  ssot_bundle_policy: ssot_bundle_policy
                        ? {
                                global_packs: globalPacks || [],
                          }
                        : null,
            },
      };
}
