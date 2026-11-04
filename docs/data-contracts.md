# Data Contracts

Schemas and artifact formats exchanged between the pipeline, the agent, the
sandbox, and the PR bot. Anything not defined here is an implementation detail.

Domain data in examples is Arabic (city and area names, payment labels) to match
the target market; field names and enums stay ASCII so tooling stays simple.

## 1. Ride/Order event model

### 1.1 Envelope

Every event on every topic uses the same envelope. Topic-specific fields live under
`payload`.

| Field            | Type     | Required | Constraints                                        |
| ---------------- | -------- | -------- | -------------------------------------------------- |
| `event_id`       | string   | yes      | `^evt_[0-9]{6,}$`, globally unique                 |
| `event_type`     | enum     | yes      | see 1.2                                            |
| `schema_version` | string   | yes      | semver `MAJOR.MINOR.PATCH`, see section 2           |
| `occurred_at`    | RFC 3339 | yes      | UTC, `Z` suffix, no future timestamps > 5 min       |
| `produced_at`    | RFC 3339 | yes      | UTC, `occurred_at <= produced_at <= now + 5 min`    |
| `producer`       | string   | yes      | service name, `^[a-z][a-z0-9-]{2,31}$`              |
| `payload`        | object   | yes      | shape determined by `event_type`                   |

Example — completed ride:

```json
{
  "event_id": "evt_100245",
  "event_type": "ride_completed",
  "schema_version": "1.4.0",
  "occurred_at": "2026-11-03T10:30:00Z",
  "produced_at": "2026-11-03T10:30:02Z",
  "producer": "ride-service",
  "payload": {
    "ride_id": "ride_88213",
    "user_id": "user_4412",
    "driver_id": "driver_917",
    "city": "الرياض",
    "pickup_area": "العليا",
    "dropoff_area": "الملز",
    "distance_km": 7.4,
    "duration_min": 21,
    "fare_amount": 38.5,
    "currency": "SAR",
    "payment_method": "بطاقة"
  }
}
```

Example — placed order:

```json
{
  "event_id": "evt_100246",
  "event_type": "order_placed",
  "schema_version": "1.4.0",
  "occurred_at": "2026-11-03T10:31:00Z",
  "produced_at": "2026-11-03T10:31:01Z",
  "producer": "order-service",
  "payload": {
    "order_id": "order_55210",
    "user_id": "user_4412",
    "merchant_id": "merchant_331",
    "city": "جدة",
    "district": "الروضة",
    "items": [
      { "sku": "sku_8841", "name": "قهوة عربية", "quantity": 2, "unit_price": 14.0 },
      { "sku": "sku_8842", "name": "تمر سكري", "quantity": 1, "unit_price": 32.5 }
    ],
    "total_amount": 60.5,
    "currency": "SAR",
    "payment_method": "الدفع عند الاستلام"
  }
}
```

### 1.2 Event types and payloads

| `event_type`     | Topic           | Key payload fields                                                                 | Notes                              |
| ---------------- | --------------- | ---------------------------------------------------------------------------------- | ---------------------------------- |
| `ride_requested` | `ride_events`   | `ride_id`, `user_id`, `city`, `pickup_area`, `requested_at`                         | No fare yet                        |
| `ride_completed` | `ride_events`   | `ride_id`, `user_id`, `driver_id`, `city`, `distance_km`, `duration_min`, `fare_amount`, `currency`, `payment_method` | Fare required                      |
| `ride_cancelled` | `ride_events`   | `ride_id`, `user_id`, `city`, `reason`, `cancelled_at`                              | Excluded from revenue aggregates   |
| `order_placed`   | `order_events`  | `order_id`, `user_id`, `merchant_id`, `city`, `items[]`, `total_amount`, `currency` | `items[]` at least one             |
| `order_cancelled`| `order_events`  | `order_id`, `reason`, `cancelled_at`                                                | Excluded from gross sales          |
| `payment_captured` | `payment_events` | `payment_id`, `event_id` (parent), `amount`, `currency`, `method`                | Must reference a parent event      |
| `payment_failed` | `payment_events` | `payment_id`, `event_id` (parent), `amount`, `failure_code`                        | Retried upstream, never deduped away |

Field type rules that matter for drift detection:

- Money: JSON number, two decimal places, stored as `NUMERIC(12,2)`. Never float
  in the warehouse.
- `currency`: ISO 4217, currently `SAR`, `AED`, `USD`.
- Timestamps: RFC 3339 UTC; local timezones are a presentation concern.
- `city` / area names: UTF-8 Arabic text, no transliteration.
- Unknown fields: consumers ignore them (forward compatibility).
- Unknown `event_type`: routed to the dead-letter topic, not dropped.

## 2. Schema versioning strategy

### 2.1 Version placement

`schema_version` travels on every event. The registry stores schemas at
`schemas/<topic>/v<MAJOR>/<MAJOR>.<MINOR>.<PATCH>.json`.

### 2.2 Compatibility rules

| Change                                           | Version bump | Allowed                 |
| ------------------------------------------------ | ------------ | ----------------------- |
| Add optional field                               | MINOR        | yes                     |
| Add required field                               | MAJOR        | yes, with migration     |
| Widen a type (int → decimal)                     | MINOR        | yes                     |
| Narrow a type (string → int)                     | MAJOR        | yes, with migration     |
| Rename a field                                   | MAJOR        | yes, dual-write window  |
| Remove a field                                   | MAJOR        | yes, after deprecation  |
| Change enum member meaning                       | MAJOR        | yes, with review        |
| Add enum member                                  | MINOR        | yes                     |

Registry compatibility mode: `BACKWARD_TRANSITIVE`. A producer that cannot publish
a backward-compatible schema must not deploy.

### 2.3 Consumer support window

Consumers must accept the current MAJOR and the previous MAJOR during a migration
window of two releases. On unknown MAJOR: reject the event to the dead-letter
topic with `unsupported_schema_major`. On unknown MINOR: process and ignore
unknown fields.

### 2.4 Renames and dual-write

A rename is never a silent change:

1. Producer emits both old and new fields for one release window.
2. Consumer accepts either, preferring the new field when both are present.
3. Producer drops the old field in the next MAJOR.
4. Registry marks the old schema deprecated.

Schema drift incidents exist precisely because this process can be skipped
upstream. Detection and the expected fix derive from the registry diff.

## 3. Canonical output data model

Cleaned tables (Postgres; identical DDL semantics in DuckDB):

### 3.1 `ride_events_clean`

| Column            | Type           | Constraints                     |
| ----------------- | -------------- | ------------------------------- |
| `event_id`        | text           | primary key                     |
| `ride_id`         | text           | not null                        |
| `user_id`         | text           | not null                        |
| `driver_id`       | text           | nullable for non-completed rides|
| `event_type`      | text           | not null, enum                  |
| `city`            | text           | not null                        |
| `distance_km`     | numeric(8,2)   | `>= 0`                          |
| `duration_min`    | integer        | `>= 0`                          |
| `fare_amount`     | numeric(12,2)  | `>= 0`                          |
| `currency`        | char(3)        | not null                        |
| `payment_method`  | text           | not null                        |
| `occurred_at`     | timestamptz    | not null                        |
| `processed_at`    | timestamptz    | not null, default now()         |
| `schema_version`  | text           | not null                        |
| `source_topic`    | text           | not null                        |
| `batch_id`        | text           | not null, indexed               |

Deduplication key: `event_id` (first occurrence wins, ordered by `produced_at`).

### 3.2 `order_events_clean`

| Column         | Type          | Constraints        |
| -------------- | ------------- | ------------------ |
| `event_id`     | text          | primary key        |
| `order_id`     | text          | not null           |
| `user_id`      | text          | not null           |
| `merchant_id`  | text          | not null           |
| `city`         | text          | not null           |
| `item_count`   | integer       | `>= 1`             |
| `total_amount` | numeric(12,2) | `>= 0`             |
| `currency`     | char(3)       | not null           |
| `occurred_at`  | timestamptz   | not null           |
| `processed_at` | timestamptz   | not null           |
| `batch_id`     | text          | not null, indexed  |

`items[]` is flattened into `order_items_clean` (`event_id`, `sku`, `name`,
`quantity`, `unit_price`) with a foreign key to `order_events_clean.event_id`.

### 3.3 `quarantine_events`

Rejected rows retain full provenance so an escalation can show them.

| Column           | Type        | Notes                                     |
| ---------------- | ----------- | ----------------------------------------- |
| `event_id`       | text        | nullable if the envelope itself was broken |
| `source_topic`   | text        | not null                                  |
| `raw_payload`    | jsonb       | PII-masked                                |
| `reason_code`    | text        | enum below                                |
| `reason_detail`  | text        | human-readable                            |
| `batch_id`       | text        | not null                                  |
| `quarantined_at` | timestamptz | not null                                  |

`reason_code` enum: `parse_error`, `missing_required_field`, `type_mismatch`,
`invalid_enum`, `invalid_range`, `unsupported_schema_major`, `duplicate_event`.

### 3.4 `daily_city_metrics`

| Column           | Type          | Notes                          |
| ---------------- | ------------- | ------------------------------ |
| `metric_date`    | date          | local business date (Asia/Riyadh) |
| `city`           | text          | not null                       |
| `rides_count`    | integer       | completed rides                |
| `gross_fare_sar` | numeric(14,2) | sum of `fare_amount`           |
| `avg_duration_min` | numeric(6,2) | completed rides only          |
| `distinct_users` | integer       | distinct `user_id`             |
| `null_city_rows` | integer       | observability only             |

Grain: one row per `(metric_date, city)`. Late events within the watermark update
the row; events older than the watermark are quarantined with
`reason_code=late_event` (see constraint DQ-007).

## 4. Data quality constraints

Severity `critical` fails the batch and raises an incident; `warning` is recorded
and attached as evidence only.

| ID     | Expectation                                    | Column / scope          | Threshold                   | Severity |
| ------ | ---------------------------------------------- | ----------------------- | --------------------------- | -------- |
| DQ-001 | `event_id` not null                            | `event_id`              | 0 nulls                     | critical |
| DQ-002 | `event_id` unique per batch                    | `event_id`              | 0 duplicates                | critical |
| DQ-003 | Fare is numeric and non-negative               | `fare_amount`           | `0 <= x <= 10000`           | critical |
| DQ-004 | Currency is an allowed ISO code                | `currency`              | `{SAR, AED, USD}`           | critical |
| DQ-005 | Payment method is a known value                | `payment_method`        | allowed set                 | warning  |
| DQ-006 | City present                                   | `city`                  | null ratio ≤ 0.5%           | critical |
| DQ-007 | Event not older than the watermark             | `occurred_at`           | ≥ `now - 48h`               | warning  |
| DQ-008 | Batch volume sane                              | row count               | within ±40% of 7-day median | warning  |
| DQ-009 | Duplicate rate low                             | batch                   | ≤ 0.1%                      | warning  |
| DQ-010 | Freshness                                      | max(`processed_at`)     | ≤ 15 minutes old            | critical |
| DQ-011 | Order items present                            | `order_events_clean`    | `item_count >= 1`           | critical |
| DQ-012 | Payment references a parent event              | `payment_events`        | 100% resolvable             | critical |

Rules:

- Expectations are versioned and are **not** editable by the agent except through
  the allowlisted expectations path, and never to weaken a critical check
  (`safety-design.md#validation-weakening`).
- Thresholds are configuration, not prose: DQ-003's bound lives in
  `pipeline/expectations/rides.yaml`.
- Every failed expectation emits `{id, column, observed, expected, severity,
  sample_ref}` into the evidence bundle.

## 5. Incident metadata

Every incident is one YAML/JSON record, stored under `incidents/` (benchmark) or
the operational store (runtime).

| Field              | Type     | Required | Notes                                        |
| ------------------ | -------- | -------- | -------------------------------------------- |
| `incident_id`      | string   | yes      | `INC-<YYYYMMDD>-<seq>` or `INC-<slug>-<seq>` |
| `detected_at`      | RFC 3339 | yes      | When the signal crossed threshold            |
| `failure_type`     | enum     | yes      | category enum below, or `unknown`            |
| `severity`         | enum     | yes      | `critical`, `high`, `warning`                |
| `status`           | enum     | yes      | `open`, `triaging`, `patching`, `verifying`, `pr_open`, `escalated`, `resolved`, `rejected` |
| `source`           | object   | yes      | `{task, dag, run_id, batch_id, commit}`      |
| `signal`           | object   | yes      | `{type, observed, expected}`                 |
| `evidence_refs`    | object   | yes      | `{logs, schema_diff, bad_rows, metrics}`     |
| `ground_truth_ref` | string   | benchmark only | Path to the labeled incident           |

Failure category enum: `schema_drift`, `type_change`, `new_enum_value`,
`duplicate_events`, `late_data`, `null_explosion`, `bad_timezone`,
`upstream_outage`, `unknown`.

Example:

```yaml
incident_id: INC-20261103-0007
detected_at: 2026-11-03T10:32:11Z
failure_type: schema_drift
severity: high
status: open
source:
  task: process_ride_events
  dag: ride_pipeline_hourly
  run_id: manual__2026-11-03T10:00:00Z
  batch_id: batch_104
  commit: 4f2c9ab
signal:
  type: task_failed
  observed: KeyError: 'amount'
  expected: success
evidence_refs:
  logs: evidence/INC-20261103-0007/logs.jsonl
  schema_diff: evidence/INC-20261103-0007/schema_diff.json
  bad_rows: evidence/INC-20261103-0007/bad_rows.json
  metrics: evidence/INC-20261103-0007/metrics.json
ground_truth_ref: data/incidents/schema_drift_001.yaml
```

## 6. Diagnostic report

Triage output. Consumed programmatically, never free text.

```json
{
  "incident_id": "INC-20261103-0007",
  "category": "schema_drift",
  "root_cause": "The field 'amount' was renamed to 'total_amount' in ride_events",
  "confidence": 0.94,
  "fixable": true,
  "evidence": [
    "KeyError: 'amount' in process_ride_events at batch_104",
    "Schema diff: amount -> total_amount (MAJOR)",
    "Last successful run (run_103) used schema 1.4.0 with 'amount'"
  ],
  "affected_components": ["pipeline/transforms/rides.py"],
  "suggested_fix_summary": "Read total_amount and update the column mapping",
  "escalation_reason": null
}
```

Field rules:

| Field                    | Rule                                                              |
| ------------------------ | ----------------------------------------------------------------- |
| `category`               | Must be in the enum; `unknown` is permitted and forces escalation |
| `confidence`             | `0.0–1.0`; below `0.70` forces escalation                         |
| `fixable`                | `false` forces escalation regardless of confidence                |
| `evidence`               | At least one item; each must reference real evidence artifact     |
| `affected_components`    | Paths from the current commit; must exist                         |
| `suggested_fix_summary`  | ≤ 240 characters                                                  |
| `escalation_reason`      | Required when `fixable=false` or confidence below threshold       |

## 7. Generated patch

A patch is an artifact, not an applied change.

```json
{
  "incident_id": "INC-20261103-0007",
  "base_commit": "4f2c9ab",
  "attempt": 1,
  "files": [
    {
      "path": "pipeline/transforms/rides.py",
      "unified_diff": "--- a/pipeline/transforms/rides.py\n+++ b/pipeline/transforms/rides.py\n@@ -12,7 +12,7 @@\n-    amount = event[\"amount\"]\n+    amount = safe_cast(event[\"total_amount\"], int)\n"
    }
  ],
  "stats": { "files_changed": 1, "lines_added": 6, "lines_removed": 2 },
  "rationale": "Map renamed field and guard the cast against non-numeric input.",
  "risk": "low",
  "rollback": "Revert the single commit on branch heal/INC-20261103-0007."
}
```

Hard rules (enforced by the sandbox and the patch validator):

- Unified diff that applies cleanly to `base_commit`; no binary or generated files.
- `files_changed ≤ 2`, `lines_added + lines_removed ≤ 30`.
- Every `path` matches the allowlist and none matches the blocklist
  (`safety-design.md#allowed-paths`).
- No change may remove, weaken, or skip a quality expectation or test.
- The patch may not modify its own guardrails, CI, or dependency manifests.

## 8. Verification result

Produced by the sandbox for each attempt.

```json
{
  "incident_id": "INC-20261103-0007",
  "attempt": 1,
  "patch_sha256": "b6f1c0d4e0e4b0f8f0f0b2c1a9d8e7f6",
  "verdict": "pass",
  "unit_tests": { "suite": "tests/unit", "passed": 24, "failed": 0, "duration_s": 12.4, "report": "artifacts/junit.xml" },
  "regression": { "suite": "tests/integration", "passed": 41, "failed": 0, "duration_s": 60.1 },
  "replay": {
    "batch_id": "batch_104",
    "processed": 5000,
    "rejected": 0,
    "golden_match": true,
    "diff_rows": 0,
    "output_ref": "artifacts/replay_batch_104.parquet"
  },
  "data_quality": { "checks_run": 12, "failed": [], "verdict": "pass" },
  "duration_s": 95.2,
  "failure_reason": null,
  "sandbox_image": "pipeline-healer-sandbox@sha256:...",
  "timestamp": "2026-11-03T10:41:52Z"
}
```

Rules:

- `verdict=pass` requires all of: `unit_tests.failed == 0`,
  `regression.failed == 0`, `replay.golden_match == true`,
  `data_quality.failed` empty.
- `verdict=fail` requires a `failure_reason` and feeds the next patch attempt.
- `verdict=error` means infrastructure failure; it does not consume a patch
  attempt unless it repeats.
- `patch_sha256` binds the verdict to the exact diff; a re-issued patch needs a new
  verification.

## 9. Pull request report

Rendered by the PR bot into the PR body. Sections are fixed so reviewers can skim.

| Section        | Content                                                             |
| -------------- | ------------------------------------------------------------------- |
| Header         | Incident id, category, severity, attempt number                     |
| Diagnosis      | Root cause and confidence, with the evidence list                   |
| Evidence       | Log excerpts, schema diff, failing-row sample (masked), metrics      |
| Proposed change| Files and summary; link to the diff                                 |
| Verification   | Unit tests, regression suite, replay result, data-quality checks     |
| Risk           | `low` / `medium` / `high` per the patch metadata, with justification |
| Rollback       | Exact instruction (revert commit on the branch)                     |
| Limits         | Confirmation that size, path, and validation guardrails passed      |

The report must never claim a check ran that is absent from the verification
result.

## 10. Cross-references

- Escalation conditions, retry limit, patch size, allowed paths:
  [`safety-design.md`](safety-design.md)
- Node-level behavior and state schema: [`agent-workflow.md`](agent-workflow.md)
- Metric definitions: [`evaluation-plan.md`](evaluation-plan.md)
- System boundaries: [`architecture.md`](architecture.md#system-boundaries)
