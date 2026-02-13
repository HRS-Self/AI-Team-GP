#!/usr/bin/env bash
set -euo pipefail

echo "[verify] Running Gap 2 hardening checks..."

node --test \
  test/patch-plan-invalid-stops-bundle.test.js \
  test/ssot-drift-blocks-auto-approve.test.js \
  test/knowledge-exports-ledger.test.js

echo "[verify] PASS"

