#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${AI_PROJECT_ROOT:-}" ]]; then
  echo "ERROR: AI_PROJECT_ROOT is required (example: /opt/AI-Projects/<code>/ops)" >&2
  exit 2
fi

echo "AI_PROJECT_ROOT=$AI_PROJECT_ROOT"

echo ""
echo "1) Validate project"
node src/cli.js --validate

echo ""
echo "2) Enqueue test intake"
node src/cli.js --enqueue "Test watchdog pipeline for dp-frontend-portal README first line date on develop"

echo ""
echo "3) Run watchdog (limit 1)"
node src/cli.js --watchdog --limit 1

echo ""
echo "4) Show schedule + latest work status"
if [[ -f "${AI_PROJECT_ROOT}/ai/lane_b/schedule/SCHEDULE.json" ]]; then
  echo "Schedule: ${AI_PROJECT_ROOT}/ai/lane_b/schedule/SCHEDULE.json"
  tail -n 40 "${AI_PROJECT_ROOT}/ai/lane_b/schedule/SCHEDULE.json" || true
else
  echo "ERROR: Missing ${AI_PROJECT_ROOT}/ai/lane_b/schedule/SCHEDULE.json" >&2
  exit 1
fi

if command -v rg >/dev/null 2>&1; then
  LATEST_WORK_ID="$(ls -1 "${AI_PROJECT_ROOT}/ai/lane_b/work" | rg '^W-' | sort | tail -n 1 || true)"
else
  LATEST_WORK_ID="$(ls -1 "${AI_PROJECT_ROOT}/ai/lane_b/work" | grep -E '^W-' | sort | tail -n 1 || true)"
fi
if [[ -z "$LATEST_WORK_ID" ]]; then
  echo "ERROR: No work item found under ${AI_PROJECT_ROOT}/ai/lane_b/work" >&2
  exit 1
fi

STATUS_PATH="${AI_PROJECT_ROOT}/ai/lane_b/work/${LATEST_WORK_ID}/STATUS.md"
echo ""
echo "Latest work: $LATEST_WORK_ID"
echo "Status: $STATUS_PATH"
cat "$STATUS_PATH" | sed -n '1,80p' || true

APPROVAL_JSON="${AI_PROJECT_ROOT}/ai/lane_b/work/${LATEST_WORK_ID}/APPROVAL.json"
if [[ -f "$APPROVAL_JSON" ]]; then
  echo ""
  echo "APPROVAL.json exists: $APPROVAL_JSON"
  cat "$APPROVAL_JSON" || true
else
  REPORT="${AI_PROJECT_ROOT}/ai/lane_b/work/${LATEST_WORK_ID}/failure-reports/watchdog.md"
  if [[ -f "$REPORT" ]]; then
    echo ""
    echo "Watchdog did not request approval; failure report:"
    echo "$REPORT"
    cat "$REPORT" || true
    exit 1
  fi
  echo ""
  echo "ERROR: APPROVAL.json missing and no watchdog failure report found." >&2
  exit 1
fi
