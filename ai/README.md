# AI Artifacts

This folder is the Orchestrator-owned ledger for planning, status reporting, risks, and decision tracking.

## How to use (MVP v0)

- Drop a new request into `ai/inbox/` as a `.md` file, or run the CLI with `--text`.
- The Orchestrator creates a work item in `ai/work/<id>/` and updates top-level ledgers.

## Conventions

- **CI is source of truth**: artifacts track intent and decisions; code + CI validate reality.
- **Scoped work**: assignments include a bounded context to avoid “one agent touches everything”.

