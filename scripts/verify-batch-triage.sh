#!/usr/bin/env bash
set -euo pipefail

ENGINE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${AI_PROJECT_ROOT:-}" ]]; then
  BASE_ROOT="/tmp/ai-project-batch-triage-verify-$(date -u +%Y%m%dT%H%M%SZ)"
  AI_PROJECT_ROOT="${BASE_ROOT}/ops"
  export AI_PROJECT_ROOT
  echo "AI_PROJECT_ROOT not set; using ${AI_PROJECT_ROOT}"
fi

if [[ "$(basename "$AI_PROJECT_ROOT")" != "ops" ]]; then
  echo "ERROR: AI_PROJECT_ROOT must end with /ops (got: ${AI_PROJECT_ROOT})" >&2
  exit 2
fi

PROJECT_HOME="$(cd "${AI_PROJECT_ROOT}/.." && pwd)"
REPOS_ROOT="${PROJECT_HOME}/repos"
KNOWLEDGE_ROOT="${PROJECT_HOME}/knowledge"

mkdir -p "${AI_PROJECT_ROOT}/config" "${AI_PROJECT_ROOT}/ai/lane_b/inbox" "${REPOS_ROOT}" "${KNOWLEDGE_ROOT}"

cat > "${AI_PROJECT_ROOT}/config/PROJECT.json" <<JSON
{
  "version": 4,
  "project_code": "verify",
  "repos_root_abs": "${REPOS_ROOT}",
  "ops_root_abs": "${AI_PROJECT_ROOT}",
  "knowledge_repo_dir": "${KNOWLEDGE_ROOT}",
  "ssot_bundle_policy": { "global_packs": [] }
}
JSON

# Minimal project config for triage+sweep (no LLM required).
cat > "${AI_PROJECT_ROOT}/config/REPOS.json" <<JSON
{
  "version": 1,
  "base_dir": "${REPOS_ROOT}",
  "repos": [
    {
      "repo_id": "demo-frontend",
      "name": "Demo Frontend",
      "path": "demo-frontend",
      "status": "active",
      "team_id": "FrontendApp",
      "Kind": "App",
      "IsHexa": false,
      "keywords": ["demo", "frontend"],
      "active_branch": "develop"
    },
    {
      "repo_id": "demo-backend",
      "name": "Demo Backend",
      "path": "demo-backend",
      "status": "active",
      "team_id": "BackendPlatform",
      "Kind": "Service",
      "IsHexa": false,
      "keywords": ["demo", "backend"],
      "active_branch": "develop"
    }
  ]
}
JSON

cat > "${AI_PROJECT_ROOT}/config/TEAMS.json" <<'JSON'
{
  "version": 1,
  "teams": [
    { "team_id": "FrontendApp", "description": "Frontend team", "scope_hints": ["frontend"], "risk_level": "normal" },
    { "team_id": "BackendPlatform", "description": "Backend team", "scope_hints": ["backend"], "risk_level": "normal" }
  ]
}
JSON

cat > "${AI_PROJECT_ROOT}/config/AGENTS.json" <<'JSON'
{
  "version": 3,
  "agents": [
    { "agent_id": "FrontendApp__planner__01", "team_id": "FrontendApp", "role": "planner", "implementation": "llm", "llm_profile": "planner.code_generation", "capacity": 1, "enabled": true },
    { "agent_id": "FrontendApp__applier__01", "team_id": "FrontendApp", "role": "applier", "implementation": "code", "capacity": 1, "enabled": true },
    { "agent_id": "BackendPlatform__planner__01", "team_id": "BackendPlatform", "role": "planner", "implementation": "llm", "llm_profile": "planner.code_generation", "capacity": 1, "enabled": true },
    { "agent_id": "BackendPlatform__applier__01", "team_id": "BackendPlatform", "role": "applier", "implementation": "code", "capacity": 1, "enabled": true }
  ]
}
JSON

cat > "${AI_PROJECT_ROOT}/config/POLICIES.json" <<'JSON'
{
  "version": 1,
  "merge_strategy": "deep_merge",
  "approval": {
    "auto_approve": {
      "enabled": false,
      "allowed_teams": [],
      "allowed_kinds": ["App", "Package"],
      "disallowed_risk_levels": ["high"],
      "require_clean_patch_plan": true
    }
  }
}
JSON

cat > "${AI_PROJECT_ROOT}/config/LLM_PROFILES.json" <<'JSON'
{
  "version": 1,
  "profiles": {
    "planner.code_generation": { "provider": "openai", "model": "gpt-5.2-codex", "options": { "reasoning": "high" } },
    "architect.interviewer": { "provider": "openai", "model": "gpt-5.2", "options": { "reasoning": "high" } },
    "architect.reviewer": { "provider": "openai", "model": "gpt-5.2", "options": { "reasoning": "high" } },
    "tech.writer": { "provider": "openai", "model": "gpt-5.1", "options": { "reasoning": "standard" } },
    "pr.description": { "provider": "openai", "model": "gpt-5.2-mini", "options": { "reasoning": "standard" } },
    "qa_test_author": { "provider": "openai", "model": "gpt-5.2-codex", "options": { "reasoning": "standard" } },
    "knowledge_scavenger": { "provider": "openai", "model": "gpt-5.2", "options": { "reasoning": "high" } }
  }
}
JSON

echo "Creating raw intake..."
RAW_INTAKE_OUT="$(node "${ENGINE_ROOT}/src/cli.js" --text "In active_branch of every active repo, create Test.txt with 'hello'." 2>&1)"
RAW_INTAKE_JSON="$(printf '%s\n' "${RAW_INTAKE_OUT}" | awk 'BEGIN{s=0} /^\{/ {s=1} {if(s) print}')"
RAW_FILE="$(node -e "const fs=require('fs'); const o=JSON.parse(fs.readFileSync(0,'utf8')); console.log(o.intake_file);" <<< "${RAW_INTAKE_JSON}")"
RAW_ID="$(basename "${RAW_FILE}" .md)"
echo "raw_intake_id=${RAW_ID}"

echo "Running triage..."
node "${ENGINE_ROOT}/src/cli.js" --triage --limit 1 >/dev/null

BATCH_PATH="${AI_PROJECT_ROOT}/ai/lane_b/inbox/triaged/BATCH-${RAW_ID}.json"
if [[ ! -f "${BATCH_PATH}" ]]; then
  echo "FAIL: missing ${BATCH_PATH}" >&2
  exit 1
fi

PROCESSED_MARKER="${AI_PROJECT_ROOT}/ai/lane_b/inbox/.processed/${RAW_ID}.json"
if [[ ! -f "${PROCESSED_MARKER}" ]]; then
  echo "FAIL: missing ${PROCESSED_MARKER}" >&2
  exit 1
fi

TRIAGED_IDS="$(node -e "const fs=require('fs'); const o=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); console.log((o.triaged_ids||[]).join('\\n'));" "${PROCESSED_MARKER}")"
if [[ -z "${TRIAGED_IDS}" ]]; then
  echo "FAIL: no triaged_ids in ${PROCESSED_MARKER}" >&2
  exit 1
fi

echo "Running sweep (creates one work per triaged item)..."
node "${ENGINE_ROOT}/src/cli.js" --sweep --limit 10 >/dev/null

FIRST_TID="$(printf '%s\n' "${TRIAGED_IDS}" | head -n 1)"
TID_MARKER="${AI_PROJECT_ROOT}/ai/lane_b/inbox/triaged/.processed/${FIRST_TID}.json"
if [[ ! -f "${TID_MARKER}" ]]; then
  echo "FAIL: missing ${TID_MARKER}" >&2
  exit 1
fi

WORK_ID="$(node -e "const fs=require('fs'); const o=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); console.log(o.work_id);" "${TID_MARKER}")"
if [[ -z "${WORK_ID}" ]]; then
  echo "FAIL: could not read work_id from ${TID_MARKER}" >&2
  exit 1
fi
echo "work_id=${WORK_ID}"

WORK_DIR="${AI_PROJECT_ROOT}/ai/lane_b/work/${WORK_ID}"
if [[ ! -d "${WORK_DIR}" ]]; then
  echo "FAIL: missing ${WORK_DIR}" >&2
  exit 1
fi

echo "OK: batch triage + sweep verified under hard-contract layout."
