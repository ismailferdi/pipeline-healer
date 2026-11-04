# Evaluation Plan

The project is only credible if it reports measured behavior on a fixed benchmark.
One nice demo proves nothing; forty labeled incidents with held-out variants do.

## 1. Hypothesis

A staged agent with sandbox verification achieves a higher correct-fix rate and a
lower unsafe-patch rate than a single-prompt LLM, at comparable or better cost per
verified fix.

Three systems are compared on the same incidents, model, and prompts budget:
regex baseline, single-prompt LLM, and the LangGraph agent.

## 2. Benchmark composition

Target: **40+ incidents** across eight failure types, five variants each, plus
negative controls.

| Failure type      | Variants | Should escalate |
| ----------------- | -------- | --------------- |
| `schema_drift`    | 5        | no              |
| `type_change`     | 5        | no              |
| `new_enum_value`  | 5        | no              |
| `duplicate_events`| 5        | no              |
| `late_data`       | 5        | no              |
| `null_explosion`  | 5        | no              |
| `bad_timezone`    | 5        | no              |
| `upstream_outage` | 5        | **yes**         |

Variants change the surface, not the category: different field names, different
topics, different magnitudes, different batch sizes. This prevents a solution that
matches one fixture string.

Negative controls: three incidents with a healthy pipeline and a mismatched alert,
expected to be closed without a patch. They catch agents that "fix" healthy code.

Recorded per incident:

- Input batch (recorded, replayable), masked sample rows.
- Log excerpts, schema diff, quality results, metrics.
- Ground-truth diagnosis, expected fix file, whether escalation is required.
- Golden output for replay comparison.
- At least one test that fails before the fix and passes after.

Incident metadata format: [`data-contracts.md#incident-metadata`](data-contracts.md#5-incident-metadata).

### Splits

| Split | Share | Purpose                                    |
| ----- | ----- | ------------------------------------------ |
| Dev   | 40%   | Prompt and rule iteration                  |
| Test  | 60%   | Held out; reported unchanged after tuning  |

Every reported number comes from the test split. Dev results may be shown only as
"tuning" and never as the headline.

## 3. Systems under test

| System           | Description                                                     |
| ---------------- | --------------------------------------------------------------- |
| Regex baseline   | Deterministic rules for schema/type errors; no LLM               |
| Single-prompt LLM| One prompt: incident + evidence → diff; no sandbox loop          |
| LangGraph agent  | Triage → patch → verify → retry/escalate, full guardrails        |

Fairness rules:

- Same model and provider for both LLM systems (one cheap model primary; a second
  stronger model reported as a separate row).
- Same evidence bundle available to both; the single-prompt system gets it in one
  message.
- Same patch limits; a single-prompt patch that exceeds limits is scored as
  unsafe, not silently truncated.
- Model version, temperature, and prompt hash recorded per run.

## 4. Metrics

Definitions used everywhere (`N` = incidents in the split):

| Metric                | Definition                                                                 | Target            |
| --------------------- | -------------------------------------------------------------------------- | ----------------- |
| Diagnosis accuracy    | `correct_category / N`                                                     | report, ≥ 0.80    |
| Fix success rate      | incidents with a `pass` verification whose replay matches golden, ` / N`    | report, ≥ 0.60    |
| Escalation precision  | `escalated_and_should / escalated_total`                                   | ≥ 0.95            |
| Escalation recall     | `escalated_and_should / should_escalate_total`                             | 1.00              |
| Unsafe patch rate     | `patches violating any guardrail / patches_produced`                       | **0.00**          |
| Regression rate       | `patches passing own tests but failing another batch / patches_passed`     | ≤ 0.05            |
| Cost per incident     | mean total spend                                                           | report            |
| Cost per verified fix | total spend / successful fixes                                             | report            |
| Latency (p50 / p95)   | wall clock per incident                                                    | report            |
| Retry distribution    | attempts 1 / 2 / 3 / escalated                                             | report            |

Notes:

- A "successful fix" requires all four verification components, matching the
  sandbox verdict rules in `data-contracts.md#verification-result`.
- Unsafe patches are never counted as fixes, even if the code "works".
- Escalation on an unfixable incident is correct behavior, not failure; it is
  excluded from the fix-success denominator and reported separately.
- Cost accounting uses actual token counts × pinned prices, plus sandbox CPU time
  if a local model is used.

## 5. Procedure

1. Reset the environment: fresh database volume, reseeded fixtures, recorded
   batches only.
2. For each incident in the split, run the system under test through the
   evaluation harness, which will expose
   `make eval-incident INCIDENT=<id> CONFIG=<system>` once implemented.
3. Capture: diagnosis, patch, verification result, PR payload (dry-run), trace,
   token counts, wall clock.
4. Score against ground truth automatically; no manual interpretation of
   free-text.
5. Repeat each configuration **three times** with different seeds; report mean,
   standard deviation, and per-run values.
6. Store raw results under `evals/reports/<config>/<timestamp>/` so any table can
   be reconstructed.

The harness must run each incident in isolation (fresh sandbox, no cached model
state) to avoid cross-contamination.

## 6. Result reporting

| Metric                | Regex          | Single-prompt LLM | LangGraph agent |
| --------------------- | -------------- | ----------------- | --------------- |
| Diagnosis accuracy    | —              | —                 | —               |
| Fix success rate      | —              | —                 | —               |
| Escalation precision  | —              | —                 | —               |
| Escalation recall     | —              | —                 | —               |
| Unsafe patch rate     | —              | —                 | —               |
| Regression rate       | —              | —                 | —               |
| Cost per incident     | —              | —                 | —               |
| Cost per verified fix | —              | —                 | —               |
| p50 / p95 latency     | —              | —                 | —               |

Rules for filling it:

- Every cell is a measured value or an explicit dash. No projections, no
  "expected" numbers presented as results.
- Include variance across runs; a single run is not a result.
- Keep the raw report directory linked from the table.
- Report the model name, version, and date; a rerun on a new model is a new row.

## 7. Acceptance criteria

MVP (four failure types, Python + DuckDB):

- Fix success ≥ 60% on dev incidents.
- Unsafe patch rate = 0.
- Escalation recall = 100% on upstream outages.
- No regression on the healthy-pipeline controls.

Full benchmark:

- Fix success ≥ 60% on the held-out split.
- Unsafe patch rate = 0.
- Escalation precision ≥ 0.95 and recall = 1.00.
- Regression rate ≤ 0.05.
- LangGraph agent beats the single-prompt system on fix success **or** matches it
  with a materially lower unsafe/regression rate. If it does not, that is the
  finding — report it.

## 8. Cost model

```text
cost_per_incident = (triage_tokens × price_in + triage_tokens_out × price_out)
                  + sum(patch_attempts)  # same formula
                  + sandbox_runtime_s × cpu_cost_per_s
                  + infra_overhead
```

Report separately: cost split by stage, retry overhead, and the marginal cost of
the third attempt. The verifier is only justified if its accuracy gain survives
that accounting.

## 9. Threats to validity

| Threat                     | Mitigation                                                    |
| -------------------------- | ------------------------------------------------------------- |
| Overfitting to fixtures    | Held-out split; variants change names and magnitudes          |
| Non-deterministic models   | 3 runs, multiple seeds, report variance                       |
| Golden output drift        | Regenerate fixtures deliberately and bump their version       |
| Small sample size          | Report confidence intervals; avoid over-claiming small gaps   |
| Model/version changes      | Pin model ids; rerun and label model version in results       |
| Benchmark leakage          | Agent never sees `ground_truth_ref` or test split labels      |
| Sandbox differences        | Identical pinned image for all systems                        |

## 10. Reproducibility checklist

- Incident fixtures and golden outputs versioned in `data/incidents/`.
- Seeds recorded per run.
- Model id, temperature, and prompt hashes recorded.
- Sandbox image digest recorded.
- Raw traces and artifacts retained under `evals/reports/`.
- One command will reproduce the headline table: `make eval` (planned).

## 11. Related documents

- Artifact formats: [`data-contracts.md`](data-contracts.md)
- Guardrails being measured: [`safety-design.md`](safety-design.md)
- Agent states and trace fields: [`agent-workflow.md`](agent-workflow.md)
- System context: [`architecture.md`](architecture.md)
