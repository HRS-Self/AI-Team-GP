import { existsSync } from 'node:fs';
import { resolve } from 'node:path';

import dotenv from 'dotenv';

function loadDotEnvIfExists(pathAbs) {
      const p = resolve(String(pathAbs || ''));
      if (!p) return false;
      if (!existsSync(p)) return false;
      // Do not override already-set env vars (explicit in code; also the default behavior).
      dotenv.config({ path: p, override: false });
      return true;
}

// Highest priority: explicit path (pm2, docker, etc.)
if (
      typeof process.env.DOTENV_CONFIG_PATH === 'string' &&
      process.env.DOTENV_CONFIG_PATH.trim()
) {
      loadDotEnvIfExists(process.env.DOTENV_CONFIG_PATH.trim());
}

if (
      typeof process.env.PROJECT_ROOT === 'string' &&
      process.env.PROJECT_ROOT.trim()
) {
      process.stderr.write(
            'PROJECT_ROOT is deprecated.\n' +
                  'Set AI_PROJECT_ROOT to OPS_ROOT instead (e.g. /opt/AI-Projects/<code>/ops) and rerun.\n',
      );
      process.exit(2);
}

if (
      typeof process.env.AI_PROJECT_ROOT === 'string' &&
      process.env.AI_PROJECT_ROOT.trim()
) {
      loadDotEnvIfExists(resolve(process.env.AI_PROJECT_ROOT.trim(), '.env'));
}

// Fallback: load engine repo .env (cwd) for developer convenience.
loadDotEnvIfExists(resolve(process.cwd(), '.env'));

const { orchestrateFromArgs } = await import('./cli/entry.js');
await orchestrateFromArgs(process.argv.slice(2));
