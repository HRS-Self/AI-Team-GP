#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TMP_BASE="$(mktemp -d /tmp/ai-team-seeds-gaps-XXXXXX)"
PROJECT_HOME="$TMP_BASE/project"
OPS_ROOT="$PROJECT_HOME/ops"
REPOS_ROOT="$PROJECT_HOME/repos"
KNOWLEDGE_ROOT="$PROJECT_HOME/knowledge"

cleanup() { rm -rf "$TMP_BASE" >/dev/null 2>&1 || true; }
trap cleanup EXIT

mkdir -p "$OPS_ROOT/config" "$OPS_ROOT/ai/lane_b/inbox" "$OPS_ROOT/ai/lane_b/cache"
mkdir -p "$REPOS_ROOT"
mkdir -p "$KNOWLEDGE_ROOT"

git -C "$KNOWLEDGE_ROOT" init -q
git -C "$KNOWLEDGE_ROOT" config user.email "verify@example.com"
git -C "$KNOWLEDGE_ROOT" config user.name "AI-Team Verify"

mkdir -p "$KNOWLEDGE_ROOT/ssot/system"

cat >"$OPS_ROOT/config/PROJECT.json" <<JSON
{
  "version": 4,
  "project_code": "verify",
  "repos_root_abs": "${REPOS_ROOT}",
  "ops_root_abs": "${OPS_ROOT}",
  "knowledge_repo_dir": "${KNOWLEDGE_ROOT}",
  "ssot_bundle_policy": { "global_packs": [] }
}
JSON

cat >"$OPS_ROOT/config/TEAMS.json" <<'JSON'
{
  "version": 1,
  "teams": [
    { "team_id": "FrontendDP", "description": "verify", "scope_hints": ["verify"], "risk_level": "normal" }
  ]
}
JSON

cat >"$OPS_ROOT/config/REPOS.json" <<JSON
{
  "version": 1,
  "base_dir": "${REPOS_ROOT}",
  "repos": [
    { "repo_id": "dp-frontend-portal", "name": "DP_Frontend-Portal", "path": "DP_Frontend-Portal", "status": "active", "team_id": "FrontendDP", "Kind": "App", "IsHexa": false }
  ]
}
JSON

cat >"$KNOWLEDGE_ROOT/ssot/system/BACKLOG_SEEDS.json" <<'JSON'
{
  "version": 1,
  "project_code": "verify",
  "generated_at": "2026-02-04T00:00:00.000Z",
  "items": [
    {
      "seed_id": "SEED-001",
      "title": "Seed one",
      "summary": "First seed.",
      "rationale": "Because.",
      "phase": 1,
      "priority": "P0",
      "target_teams": ["FrontendDP"],
      "target_repos": null,
      "acceptance_criteria": ["A"],
      "dependencies": { "must_run_after": [], "can_run_in_parallel_with": [] },
      "ssot_refs": [],
      "confidence": 0.5
    },
    {
      "seed_id": "SEED-002",
      "title": "Seed two",
      "summary": "Second seed.",
      "rationale": "Because.",
      "phase": 2,
      "priority": "P1",
      "target_teams": ["FrontendDP"],
      "target_repos": null,
      "acceptance_criteria": ["B"],
      "dependencies": { "must_run_after": [], "can_run_in_parallel_with": [] },
      "ssot_refs": [],
      "confidence": 0.5
    }
  ]
}
JSON

cat >"$KNOWLEDGE_ROOT/ssot/system/GAPS.json" <<'JSON'
{
  "version": 1,
  "project_code": "verify",
  "baseline": "verify baseline",
  "generated_at": "2026-02-04T00:00:00.000Z",
  "items": [
    {
      "gap_id": "GAP-001",
      "title": "Gap one",
      "summary": "First gap.",
      "observed_evidence": ["E1"],
      "impact": "high",
      "risk_level": "high",
      "recommended_action": "Fix it.",
      "target_teams": ["FrontendDP"],
      "target_repos": null,
      "acceptance_criteria": ["C"],
      "dependencies": { "must_run_after": [], "can_run_in_parallel_with": [] },
      "ssot_refs": [],
      "confidence": 0.5
    }
  ]
}
JSON

export AI_PROJECT_ROOT="$OPS_ROOT"

echo "[1/4] seeds-to-intake first run"
node "$REPO_ROOT/src/cli.js" --seeds-to-intake --phase 1 --limit 1 >/dev/null

SEED_INTAKES_1="$(ls "$OPS_ROOT/ai/lane_b/inbox"/I-*.md 2>/dev/null | wc -l | tr -d ' ')"
PROMO_LINES_1="$(wc -l <"$OPS_ROOT/ai/lane_b/cache/promotions.jsonl" | tr -d ' ')"
if [[ "$SEED_INTAKES_1" != "1" ]]; then
  echo "FAIL: expected 1 intake after first seeds-to-intake run; got $SEED_INTAKES_1"
  exit 1
fi
if [[ "$PROMO_LINES_1" != "1" ]]; then
  echo "FAIL: expected 1 promotion record after first seeds-to-intake run; got $PROMO_LINES_1"
  exit 1
fi

echo "[2/4] seeds-to-intake second run (idempotent)"
node "$REPO_ROOT/src/cli.js" --seeds-to-intake --phase 1 --limit 1 >/dev/null

SEED_INTAKES_2="$(ls "$OPS_ROOT/ai/lane_b/inbox"/I-*.md 2>/dev/null | wc -l | tr -d ' ')"
PROMO_LINES_2="$(wc -l <"$OPS_ROOT/ai/lane_b/cache/promotions.jsonl" | tr -d ' ')"
if [[ "$SEED_INTAKES_2" != "1" ]]; then
  echo "FAIL: expected 1 intake after second seeds-to-intake run; got $SEED_INTAKES_2"
  exit 1
fi
if [[ "$PROMO_LINES_2" != "1" ]]; then
  echo "FAIL: expected promotions ledger line count to remain 1; got $PROMO_LINES_2"
  exit 1
fi

echo "[3/4] gaps-to-intake first run"
node "$REPO_ROOT/src/cli.js" --gaps-to-intake --impact high --limit 1 >/dev/null

GAP_INTAKES_1="$(ls "$OPS_ROOT/ai/lane_b/inbox"/I-*.md 2>/dev/null | wc -l | tr -d ' ')"
PROMO_LINES_3="$(wc -l <"$OPS_ROOT/ai/lane_b/cache/promotions.jsonl" | tr -d ' ')"
if [[ "$GAP_INTAKES_1" != "2" ]]; then
  echo "FAIL: expected 2 intakes after gaps-to-intake run; got $GAP_INTAKES_1"
  exit 1
fi
if [[ "$PROMO_LINES_3" != "2" ]]; then
  echo "FAIL: expected 2 promotion records after gaps-to-intake run; got $PROMO_LINES_3"
  exit 1
fi

echo "[4/4] gaps-to-intake second run (idempotent)"
node "$REPO_ROOT/src/cli.js" --gaps-to-intake --impact high --limit 1 >/dev/null

GAP_INTAKES_2="$(ls "$OPS_ROOT/ai/lane_b/inbox"/I-*.md 2>/dev/null | wc -l | tr -d ' ')"
PROMO_LINES_4="$(wc -l <"$OPS_ROOT/ai/lane_b/cache/promotions.jsonl" | tr -d ' ')"
if [[ "$GAP_INTAKES_2" != "2" ]]; then
  echo "FAIL: expected 2 intakes after second gaps-to-intake run; got $GAP_INTAKES_2"
  exit 1
fi
if [[ "$PROMO_LINES_4" != "2" ]]; then
  echo "FAIL: expected promotions ledger line count to remain 2; got $PROMO_LINES_4"
  exit 1
fi

echo "verify-seeds-gaps-to-intake: PASS"
