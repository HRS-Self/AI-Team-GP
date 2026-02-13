# ADR 0001: Orchestrator is explicit code + artifacts

Date: 2026-01-28

## Status

Accepted

## Context

We are building an AI-driven software delivery team. Orchestration and governance must be explicit, inspectable, and auditable.

LangChain may be used for LLM calls and structured outputs, but must not define the orchestration structure.

## Decision

- Implement Orchestrator logic as explicit code with deterministic states.
- Store planning, status, risks, and decisions as durable artifacts under `ai/`.
- Add governance “agents” (Architect, Tech Writer) as event-driven signals, not always-on loops.

## Consequences

- Clear audit trail and reproducible behaviors.
- Slightly more manual wiring up-front (versus graph-based orchestration).

