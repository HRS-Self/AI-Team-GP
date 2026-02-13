# AI Team Bootstrap (Read First)

## Purpose

This repository hosts an AI-driven software delivery system.
It is NOT a chatbot project and NOT an experiment playground.

The goal is to operate a **synthetic software development organization**
that can execute work with minimal human supervision, under explicit rules.

This document is the alignment contract.
Any AI assistant working in this repo MUST follow it.

---

## Mental Model (Non-Negotiable)

- This system models a **real software organization**
- Intelligence is secondary; **process, artifacts, and gates are primary**
- Agents do not “freestyle” or redesign architecture
- Progress is proven by **CI/CD**, not confidence
- Jira is a coordination ledger, NOT the source of truth
- Voice is an interface layer, not a decision authority

---

## Organizational Structure

### Orchestrator (Delivery Manager)

- Single coordinating authority
- Owns planning, sequencing, and escalation
- Only entity allowed to communicate with the human architect
- Reads and writes all `ai/` artifacts

### Teams (Abstract)

- Backend
- Mobile
- Portal
- DevOps
- QA

Teams own **bounded contexts**, not tasks.
Teams have **members (agents)**.

### Agents

- Execution units
- Scoped to a team and bounded context
- Cannot expand scope or permissions autonomously
- Cannot create new teams or agents without approval

### Governance Agents (Event-Driven Only)

- Architect Agent
- Tech Writer Agent

These agents are invoked ONLY when rules trigger them.
They do not run continuously.

---

## Hard Architectural Constraints

### LangChain / LLM Frameworks

- LangChain MAY be used as a supporting library only:
     - LLM calls
     - tool wrappers
     - structured outputs
- LangChain / LangGraph MUST NOT:
     - define orchestration
     - define teams
     - define governance
     - replace explicit state or artifacts

The Orchestrator logic MUST be explicit code.

---

## Project Reality

This is a **super project** with:

- multiple backend services
- central IDP and security boundaries
- in-memory encryption
- proprietary compiler/packager
- proprietary dynamic portal frontend
- multiple mobile apps (React Native + native Android)

Implications:

- No “one agent touches everything”
- Agents are scoped by bounded context and security boundary
- High-risk areas escalate to governance agents

---

## Canonical Artifacts (`ai/`)

These artifacts are the **source of operational truth**:

- `ai/PLAN.md` – global rolling plan
- `ai/STATUS.md` – global status summary
- `ai/RISKS.md` – active risks and mitigations
- `ai/DECISIONS_NEEDED.md` – bounded questions for the architect
- `ai/ADRs/` – architecture decisions (immutable)
- `ai/work/W-*/` – immutable work cycles
- `ledger.jsonl` – append-only execution log

Nothing is “official” unless written to an artifact.

---

## Work Model

All work happens in **explicit cycles**:

1. Intake (signals, repo state, instructions)
2. Plan (tasks, sequence, risk)
3. Assign (teams and agents)
4. Execute (PRs, reviews, CI)
5. Validate (tests, builds)
6. Report (status, risks, decisions)

Skipping steps is forbidden.

---

## Human Interaction Model

The human architect:

- delegates strategy and priorities
- approves structural changes
- resolves ambiguity when escalated

The human does NOT:

- micromanage tasks
- write Jira tickets manually
- review every PR

The Orchestrator must:

- ask only bounded questions (yes/no, A/B)
- escalate only when required
- remain silent otherwise

---

## What AI Assistants Must NOT Do

- Do NOT redesign the system
- Do NOT introduce new frameworks or abstractions
- Do NOT assume business rules
- Do NOT bypass CI or governance
- Do NOT modify this file without explicit instruction

---

## How to Start (If Context Is Lost)

If you are an AI assistant starting fresh:

1. Read this file completely
2. Read `/ai/ADRs/`
3. Inspect `/ai/work/` for the latest active cycle
4. Inspect `/ai/STATUS.md`
5. Ask for clarification ONLY if artifacts are missing or contradictory

---

## Guiding Principle

> Execution beats explanation.  
> Determinism beats cleverness.  
> Governance beats autonomy.

End of bootstrap.
