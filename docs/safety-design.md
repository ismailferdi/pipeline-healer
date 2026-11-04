# Safety Design

The system's value is not that it writes patches; it is that unsafe patches do not
survive. Guardrails are implemented in code and verified by tests — a rule that
exists only in a prompt does not exist.

## 1. Threat model

| Threat                                        | Impact                                  | Control                                                        |
| --------------------------------------------- | --------------------------------------- | -------------------------------------------------------------- |
| Patch breaks the pipeline further             | Corrupted warehouse tables              | Sandbox replay + regression suite before any PR                |
| Patch disables a validation to "pass"         | Quality failures hidden                 | Diff analysis rejects removed/weakened checks                  |
| Patch edits infrastructure, CI, or secrets    | Supply-chain or deployment risk         | Path allowlist; blocklist for infra, secrets, workflows        |
| Patch is enormous                             | Unreviewable, high blast radius         | ≤ 30 changed lines and ≤ 2 files                               |
| Agent writes production or merges             | Unreviewed change reaches users         | No production credentials; branch protection; no merge scope    |
| Prompt injection inside logs or bad rows      | Agent follows attacker instructions     | Evidence is data, never instructions; tool output is sanitized |
| Runaway loop or spend                         | Cost, stuck incidents                   | Attempt cap, token/time/cost budgets, terminal escalation      |
| Sandbox escape or credential leak             | Host or production compromise           | No network, non-root, no secrets, no host mounts, no docker socket |
| Duplicate or spam PRs                         | Reviewer noise                          | One PR per incident id; idempotent lookup                      |
| Escalating nothing (blind fixing)             | Wrong patches on unfixable incidents    | `fixable=false` and confidence threshold force escalation      |
| Over-escalating                               | Agent provides no value                 | Escalation precision/recall measured in the benchmark          |

## 2. Enforcement layers

Defense in depth — each layer assumes the previous one can fail:

1. **Prompt** — states the rules. Necessary, never sufficient.
2. **Patch validator** — deterministic checks before the sandbox runs: size, paths,
   diff applicability, validation weakening, forbidden operations.
3. **Sandbox** — isolated execution; rejects unavailable patches and produces the
   verdict.
4. **PR bot scope** — can create branches and draft PRs only; cannot merge.
5. **Branch protection** — `main` requires a human review, blocks force-push and
   deletion, and forbids auto-merge.
6. **Audit trail** — every tool call, patch, verdict, and escalation is recorded.

## 3. Patch limits

| Limit                         | Value | Enforcement point          |
| ----------------------------- | ----- | -------------------------- |
| Max changed lines             | 30    | patch validator, sandbox   |
| Max modified files            | 2     | patch validator, sandbox   |
| Max patch attempts            | 3     | graph (Verify node)        |
| Max patch validator rejections| 2     | Patch node                 |
| Max sandbox infrastructure errors | 2 | Verify node               |
| Max PR creation retries       | 3     | PR bot                     |

## 4. Allowed paths

Globs are matched against repository-relative POSIX paths.

Allowlist:

```text
pipeline/transforms/**
pipeline/mappings/**
pipeline/expectations/**
```

Blocklist (checked first; wins on overlap):

```text
infrastructure/**
secrets/**
production/**
ci/**
.github/**
docker-compose.yml
Dockerfile*
pyproject.toml
uv.lock
Makefile
src/pipeline_healer/agent/**
src/pipeline_healer/sandbox/**
src/pipeline_healer/chaos/**
```

Rationale:

- Transforms and mappings are where the failure modes live.
- Expectations may be corrected (a wrong threshold for a real domain rule), but
  never weakened to hide a failure.
- Everything that could change the agent, its guardrails, the sandbox, or the
  build is off-limits. An agent that can edit its own constraints has none.
- `chaos/` is read-only for the agent: incidents must not be edited to match the
  patch.

Path checks run twice: before sandboxing and again inside the sandbox after
`git apply`, because a diff can rename files.

## 5. Dangerous operations

Rejected when found in an added line, a modified SQL string, or a migration:

- DDL/DML: `DROP`, `DELETE`, `TRUNCATE`, `ALTER` on warehouse objects.
- Arbitrary process or network access: `subprocess`, `os.system`, `eval`, `exec`,
  socket or HTTP client use inside transform code.
- Credential access: reading environment variables that match secret patterns,
  opening files outside the repository.
- Disabling error handling around validation: bare `except: pass` on quality code.

Checks are syntax-aware (Python AST, SQL statement parsing) rather than substring
matching, so comments or renamed wrappers do not bypass them.

## 6. Validation weakening

Any patch that does one of the following is rejected outright:

- Removes, comments out, or renames a Great Expectations expectation.
- Lowers a critical threshold (DQ-001 … DQ-012).
- Changes `severity: critical` to `warning`.
- Adds `pytest.skip`, `xfail`, or deletes/skips a test.
- Narrows the input range so failing rows no longer reach the assertion.
- Wraps a failing check in a try/except that swallows the failure.

Detection compares the parsed expectation/test registry before and after the
patch, not the raw diff text. Removing a check while adding an equivalent stronger
one is also rejected — equivalence is a human judgment, not the agent's.

## 7. Budgets

| Budget                         | Limit                          | On breach           |
| ------------------------------ | ------------------------------ | ------------------- |
| Tokens per incident            | configurable, default 200k     | escalate            |
| LLM calls per incident         | 12                             | escalate            |
| Wall clock per incident        | 20 minutes                     | escalate            |
| Sandbox runs per incident      | 3                              | escalate            |
| Estimated spend per incident   | configurable, default $0.50    | escalate            |
| Concurrent incidents           | 1 per pipeline                 | queue               |

Budget breaches escalate with partial results; they never silently continue.

## 8. Sandbox isolation

| Control            | Setting                                                       |
| ------------------ | ------------------------------------------------------------- |
| Network            | `--network none`                                              |
| Base filesystem    | read-only image; ephemeral writable layer                     |
| User               | non-root UID/GID                                              |
| Secrets            | none injected; env allowlist checked before run               |
| Mounts             | no host paths, no docker socket                               |
| Resources          | 2 CPU, 2 GB RAM, 10-minute timeout, PID limit                 |
| Image              | pinned by digest per project release                          |
| Data               | synthetic or masked; golden outputs versioned with fixtures    |
| Outputs            | collected as artifacts, container destroyed after verification |

The sandbox deliberately cannot reach the network: a patch that needs to download
something is a patch the verification cannot judge.

## 9. Escalation conditions

Escalation is a first-class outcome. Any one condition is sufficient; the reason
is recorded in the escalation package.

| #  | Condition                                                   | Detected at        |
| -- | ----------------------------------------------------------- | ------------------ |
| E1 | Triage returns `fixable=false`                              | Triage             |
| E2 | `confidence < 0.70`                                         | Triage             |
| E3 | Category is `upstream_outage` or `unknown`                  | Triage             |
| E4 | No candidate fix inside the allowlist                       | Hypothesize        |
| E5 | Patch rejected by the validator twice                       | Patch              |
| E6 | Three patch attempts without a `pass` verdict               | Verify             |
| E7 | Two consecutive sandbox `error` verdicts                    | Verify             |
| E8 | Evidence is missing or internally contradictory             | Triage             |
| E9 | Budget exceeded (tokens, calls, time, spend)                | Any node           |
| E10| PR creation fails after 3 attempts                          | OpenPR             |
| E11| Incident payload is malformed or references unknown commit  | Intake             |
| E12| Guardrail violation that is not auto-fixable                | Validator / sandbox|

Escalation must never be used as a way to avoid patching a clearly fixable,
well-evidenced incident; that is what the escalation-precision metric measures.

## 10. Human approval

Approval is mandatory, non-delegable, and enforced twice: by policy here and by
branch protection in GitHub.

- Every merge requires an approving human review from the on-call data engineer.
- The agent cannot approve, dismiss a review, mark a PR ready, or enable
  auto-merge; its token has no such scope.
- `main` protection: required review, no force-push, no deletion, no bypass for
  the bot account.
- The agent opens PRs as **draft**; a human marks ready and merges.
- PRs touching blocked paths are rejected before creation, so there is no
  "approve anyway" path.

Reviewer checklist:

1. Diagnosis matches the evidence (no unexplained leaps).
2. Diff stays within allowlist and size limits.
3. Verification result shows unit, regression, replay, and quality checks passing.
4. Replay output matches golden, not merely "tests green".
5. No check, threshold, or test was weakened.
6. Rollback instruction is valid.

## 11. Required safety tests

`tests/safety/` must prove the controls, not assume them:

- Patch with a removed expectation → rejected, incident escalated.
- Patch lowering DQ-003's bound → rejected.
- Patch adding `pytest.skip` → rejected.
- Patch touching `.github/workflows` → rejected by path check.
- Patch with 31 changed lines → rejected by size check.
- Patch containing `DROP TABLE` in a SQL string → rejected.
- Four verification attempts → graph escalates after the third.
- Upstream-outage incident → diagnosis `fixable=false`, no patch attempted.
- Sandbox with network access attempt → fails closed.
- Duplicate PR creation for one incident → single PR.

Each test asserts on the rejection reason, so failures are diagnosable.

## 12. Incident response when a guardrail trips

1. The patch is rejected; the incident keeps its status and records the reason.
2. The agent either retries within the attempt budget or escalates.
3. Exceeding a hard limit (unsafe diff, forbidden path, validation weakening) ends
   the run immediately — those are not retryable.
4. The escalation record includes the rejected diff for human inspection.

## 13. Residual risks

- **Benchmark overfitting** — the agent may learn the injected failure signatures.
  Mitigated by held-out test incidents (`evaluation-plan.md`).
- **Replay drift** — golden outputs can rot as the pipeline evolves. Fixtures are
  versioned and regenerated deliberately.
- **Masking gaps** — PII redaction is pattern-based; a new field name can leak.
  Reviewed whenever the event schema changes.
- **Model behavior drift** — providers update models. Evaluation pins model
  versions and re-runs on change.
- **Human fatigue** — reviewers may rubber-stamp. Short reports and explicit risk
  fields reduce, not eliminate, this risk.

## 14. Related documents

- Boundaries: [`architecture.md`](architecture.md#system-boundaries)
- Artifact formats: [`data-contracts.md`](data-contracts.md)
- Node behavior: [`agent-workflow.md`](agent-workflow.md)
- Measurement: [`evaluation-plan.md`](evaluation-plan.md)
