# AI-Team Project Registry

This directory stores the **global, host-local project registry** for the AI-Team tool repo.

Location (hard contract):
- `<AI_TEAM_REPO>/ai/registry/REGISTRY.json`
- `<AI_TEAM_REPO>/ai/registry/REGISTRY.lock`

Notes:
- This registry is **not per-project**. It tracks multiple `/opt/AI-Projects/<project_code>` instances on the same host.
- `REGISTRY.lock` is an advisory lock used to make port allocation and registry updates concurrency-safe.
- Writes are atomic (temp + rename).

