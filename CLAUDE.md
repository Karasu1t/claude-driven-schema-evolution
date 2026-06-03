# Claude-Driven Schema Evolution - Project Guide

## Language

Always respond in English in this project.

## Project Overview

A pipeline that semi-automates schema changes (column additions and deletions) in an ETL system.
- AWS Glue (PySpark) → Apache Iceberg → Athena
- Infrastructure: Terraform
- CI/CD: GitHub Actions (ci / unit_test / e2e_test / terraform_apply / terraform_destroy)

## Branch Strategy

- `main`: production
- `dev`: integration branch for development
- `feature/{YYYYMMDD}/{operation}_{column}`: working branches (cut from `dev`)

---

## Schema Change Workflow

Use the `/update-schema` command for all schema changes (column additions and deletions).

```
/update-schema -add video_category StringType
/update-schema -delete video_duration_minutes
/update-schema -add video_category StringType -delete video_duration_minutes
```

The command handles the full workflow in two phases:

**Phase 1 — Confirm all details before touching any files**
- Operation type (add / delete / both)
- Insert position (based on `SOURCE_SCHEMA` in `src/glue/schema.py`, source columns only — metadata columns excluded)
- Final summary with file list and purpose of each file

**Phase 2 — Execute one step at a time with confirmation**
Each step shows the exact diff and asks `Proceed? (y/n)` before making any change.

| Step | File | Purpose |
|------|------|---------|
| 1/8 | Branch creation | `feature/{YYYYMMDD}/add_{column}` from `dev` |
| 2/8 | `terraform/modules/aws/glue_table/main.tf` | Glue Catalog column definition |
| 3/8 | `test_data/sns_advertisement_yyyymmdd.csv` | E2E test input template |
| 4/8 | `test_data/expected_output.csv` | E2E expected output |
| 5/8 | `src/glue/schema.py` | Schema definition — single source of truth (imported by glue_job.py and tests) |
| 6/8 | `tests/test_glue_job.py`, `tests/test_data_transform.py` | Schema structure tests + data value tests (sample_df tuples updated manually) |
| 7/8 | `.github/workflows/e2e_test.yml` | CI config (conditional — only if timestamp type) |
| 8/8 | Commit + PR | Opens PR against `dev`, triggers Terraform apply + UT → E2E automatically |

---

## CI/CD Behavior

Opening a PR against `dev` triggers the CI pipeline (`ci.yml`):

```
pull_request → dev
    ├── unit_test.yml       (parallel)
    └── terraform_apply.yml (parallel)
              ↓ only if both pass
         e2e_test.yml
```

- `unit_test.yml` and `terraform_apply.yml` run in parallel.
- `e2e_test.yml` runs only after both succeed.
- Terraform apply runs on PR so that the Glue Catalog and Glue Job are up to date before E2E queries Athena.

---

## Execution Principles

- Never modify files without confirmation.
- Each step is independent — never batch multiple steps together.
- If an error occurs mid-step, report immediately and do not proceed.
- If `n` is answered, revise and re-present the same step.

---

## DataType Reference

| Use case | Spark StructField | Terraform type |
|----------|-------------------|----------------|
| Text     | StringType()      | string         |
| Integer  | LongType()        | bigint         |
| Decimal  | DoubleType()      | double         |
| Date     | DateType()        | date           |
| Boolean  | BooleanType()     | boolean        |
