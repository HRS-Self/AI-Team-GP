# Lane A Contracts & Invariants

This directory defines the authoritative contracts and invariants for Lane A (Knowledge) and the Lane A ↔ Lane B boundary.

These contracts are intentionally strict:
- Determinism is mandatory.
- Validation failures are fatal and explicit.
- No silent acceptance of invalid data.
- No auto-correction of missing fields.

## Invariants (documented + enforced where applicable)

1) No knowledge claim without evidence
- Any claim-like output (facts, findings, gaps, decisions) MUST reference evidence.
- Evidence references MUST be explicit; placeholders are forbidden.

2) Evidence must be resolvable
- Evidence must point to a real file path and a real commit SHA.
- A pipeline that has access to a repo working tree MUST verify that the referenced file path exists for the referenced commit.

3) System synthesis requires complete scan coverage
- System synthesis MUST be blocked until scan coverage is complete (100% of repos in scope scanned successfully).

4) Committee outputs reference evidence IDs only
- Committee outputs MUST reference `evidence_id` values.
- Committee outputs MUST NOT embed raw file paths/line ranges as evidence; those belong only in `evidence_ref` records.

5) Decision packets are the only human-interruption artifact
- The only sanctioned human-interruption artifact is a Decision Packet.
- Other ad-hoc “needs human” files are forbidden.

6) Knowledge refresh from events is incremental and repo-scoped
- Knowledge events MUST identify the repo and merge commit.
- Refresh logic MUST be incremental and must not trigger cross-repo rewrites.

7) Stable IDs only (no UUIDs)
- IDs (evidence_id, decision_id, event_id, etc.) MUST be stable and deterministic.
- Random UUIDs are forbidden unless derived from a stable salted hash.
