# Claude-Driven Schema Evolution

Schema changes in ETL pipelines are repetitive, error-prone, and expensive.
Every column addition touches multiple files — ETL logic, test data, expected outputs, infrastructure definitions — and missing even one breaks the pipeline silently.

This project solves that with **Claude as an interactive co-engineer**: a `/update-schema` command walks through each affected file step by step, shows exactly what will change, and asks for confirmation before touching anything. When done, it opens a PR that automatically triggers unit tests followed by E2E tests on AWS.

---

## Why This Exists

In a production data platform, schema changes are inevitable. The traditional approach requires a developer to:

1. Manually identify every file that references the schema
2. Apply consistent changes across ETL logic, tests, and infrastructure
3. Validate that nothing was missed

This is tedious, inconsistency-prone, and scales poorly as the pipeline grows. The cost isn't just time — it's the cognitive overhead of tracking dependencies and the risk of a silent failure in production.

**The goal**: reduce that process to a single command, with a human in the loop at every decision point.

---

## How It Works

### The `/update-schema` Command

Invoked inside a Claude Code session:

```
/update-schema -add video_category StringType
/update-schema -delete video_duration_minutes
/update-schema -add video_category StringType -delete video_duration_minutes
```

**Phase 1 — Gather information (no files touched yet)**

Claude reads the current schema from `src/glue/schema.py` (the single source of truth for column definitions), displays the column list, and asks where to insert the new column. Once confirmed, it summarizes the full changeset and waits for approval before proceeding.

**Phase 2 — Execute step by step**

Each file is handled one at a time. Before every change, Claude shows the exact diff and asks `y/n`. Answering `n` revises the proposal; only `y` moves forward.

```
[STEP 1/8] Branch creation
[STEP 2/8] terraform/modules/aws/glue_table/main.tf   ← Glue Catalog column definition
[STEP 3/8] test_data/sns_advertisement_yyyymmdd.csv   ← E2E test input
[STEP 4/8] test_data/expected_output.csv              ← E2E expected output
[STEP 5/8] src/glue/schema.py                         ← Schema definition (single source of truth)
[STEP 6/8] tests/test_glue_job.py, test_data_transform.py ← Unit tests
[STEP 7/8] .github/workflows/e2e_test.yml             ← CI config (if needed)
[STEP 8/8] Commit + PR creation                       ← Triggers automated tests
```

### CI/CD: Terraform Apply Before E2E

Opening a PR to `dev` triggers a parallel-then-sequential pipeline:

```
pull_request → dev
    ├── unit_test.yml      (schema evolution logic, Python 3.11 + 3.12)
    └── terraform_apply.yml (update Glue Catalog + deploy Glue Job script)
              ↓ only if both pass
         e2e_test.yml      (upload CSV → trigger Glue Job → query Athena → compare output)
```

Terraform apply and unit tests run in parallel. E2E only runs after both succeed — there is no value in testing AWS infrastructure against broken logic or a stale schema.

---

## Pipeline Architecture

```
CSV (sns_advertisement_YYYYMMDD.csv)
    ↓
S3 Upload
    ↓
Lambda  ─── extracts date from filename
        ─── detects test vs. production by bucket name
        ─── invokes Glue Job with --VER_DATE
    ↓
AWS Glue Job (PySpark 3.11)
    ├── reads CSV with explicit schema
    ├── handles missing columns via schema evolution
    ├── writes to Apache Iceberg v2 (partitioned by date)
    └── registers table in Glue Catalog
    ↓
Athena  ─── queryable via Glue Catalog
```

### Environment Separation

Lambda detects test vs. production by bucket name:

| | Test | Production |
|---|---|---|
| Input bucket | `dev-karasuit-test-raw-bucket` | `dev-karasuit-raw-bucket` |
| Output bucket | `dev-karasuit-test-processed-bucket` | `dev-karasuit-processed-bucket` |
| Glue database | `dev_karasuit_iceberg_db_test` | `dev_karasuit_iceberg_db` |

---

## Tech Stack

| Layer | Technology |
|---|---|
| ETL | AWS Glue 4.0 (PySpark 3.11) |
| Storage format | Apache Iceberg v2 |
| Catalog | AWS Glue Catalog |
| Orchestration | EventBridge → Lambda → Glue |
| Infrastructure | Terraform |
| CI/CD | GitHub Actions |
| AI workflow | Claude Code (`/update-schema` custom command) |

---

## Project Structure

```
.
├── src/glue/
│   ├── schema.py                      # SOURCE_SCHEMA / SOURCE_COLUMNS (single source of truth)
│   └── glue_job.py                    # PySpark ETL logic
├── terraform/
│   ├── modules/aws/
│   │   ├── glue_table/main.tf         # Glue Catalog table definition
│   │   ├── glue_job/                  # Glue Job configuration
│   │   ├── lambda_trigger/            # Lambda + IAM
│   │   ├── eventbridge/               # Scheduled trigger
│   │   └── s3_*/                      # Raw + processed buckets
│   └── env/dev/aws/main.tf
├── test_data/
│   ├── sns_advertisement_yyyymmdd.csv # E2E test input template
│   └── expected_output.csv            # E2E expected output
├── tests/
│   ├── test_glue_job.py               # Schema structure tests
│   └── test_data_transform.py         # Data value and type correctness tests
├── .github/workflows/
│   ├── ci.yml                         # PR pipeline: UT → E2E (sequential)
│   ├── unit_test.yml                  # Unit tests (also dispatchable)
│   ├── e2e_test.yml                   # E2E tests (also dispatchable)
│   ├── terraform_apply.yml
│   └── terraform_destroy.yml
└── .claude/commands/
    └── update-schema.md               # /update-schema command definition
```

---

## Schema

### Source columns (from CSV)

| Column | Type |
|---|---|
| `video_title` | STRING |
| `views` | LONG |
| `channel_name` | STRING |
| `channel_subscribers` | LONG |
| `likes` | LONG |
| `video_duration_minutes` | DOUBLE |
| `video_category` | STRING |

### Metadata columns (added by Glue Job)

| Column | Type | Description |
|---|---|---|
| `partition_date` | DATE | Extracted from filename (YYYYMMDD → YYYY-MM-DD) |
| `processed_at` | STRING | ISO timestamp of processing |

---

## Deployment

```bash
cd terraform/env/dev/aws
terraform init
terraform plan
terraform apply
```

---

## Design Decisions

**Why confirmation at every step, not full automation?**
Schema changes are infrequent and high-stakes. A fully automated approach removes the human judgment that catches contextual errors — wrong data types, misaligned sample values, test cases that pass structurally but verify the wrong behavior. The interactive model keeps the engineer accountable while eliminating the mechanical work.

**Why Iceberg over Parquet?**
ACID transactions, time-travel queries, and schema versioning without rewriting entire partitions. For a pipeline where schema evolution is a first-class concern, Iceberg is the right primitive.

**Why UT before E2E in CI?**
E2E tests invoke real AWS infrastructure (Lambda, Glue, Athena, S3). Running them against broken logic wastes time and incurs unnecessary cost. Unit tests are fast and cheap; they act as a gate.

**Why run Terraform apply on PR, not just on merge to main?**
E2E tests query actual Athena against the live Glue Catalog. Without applying the schema change first, the catalog is stale and E2E will always fail on the PR. Applying on PR is the only way to make the full CI pipeline meaningful within a single PR. Note: this project uses a single AWS account for both dev and production workflows — in a team setup, dev and prod would be separate accounts and Terraform apply on PR would target only the dev account.
