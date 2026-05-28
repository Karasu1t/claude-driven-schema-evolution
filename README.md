# Claude-Driven Schema Evolution for Data Engineering

**AI-Assisted ETL with Schema Evolution on AWS**

A practical data engineering project: CSV with changing schemas → AWS Glue (PySpark) transformation → Parquet output. Demonstrates production-ready cloud ETL architecture with potential AI integration for schema-change automation.

**Repository:** [`claude-driven-schema-evolution`](https://github.com/Karasu1t/claude-driven-schema-evolution)

---

## 日本語: プロジェクト概要

**背景:**

ビデオプラットフォームのデータフィードは CSV フォーマットで頻繁にスキーマ変更が発生する（カラム追加、削除など）。

**現状の対応プロセス:**

1. CSV スキーマ変更を検出（手動）
2. PySpark 変換コードを修正（手動）
3. テスト実施
4. AWS Glue Job を再デプロイ

**このプロジェクトの実装:**

- ✅ **AWS Glue + Lambda + EventBridge**: 毎日 6 AM UTC に自動実行される本番 ETL パイプライン
- ✅ **CSV → Parquet**: スキーマ推測 + 必須カラム検証 + 動的追加
- ✅ **Terraform**: 全 AWS リソースを Infrastructure as Code で管理
- 🔄 **拡張可能**: スキーマ変更時に Glue Job を修正・再デプロイするだけで対応可能

---

## How It Works (Current Implementation - Phase 1)

```
Automated Daily Workflow with Date-Based File Processing:

CSV File Upload (named: sns_advertisement_YYYYMMDD.csv)
  ↓
S3 PUT Event
  ↓
Lambda Function (dev-karasuit-glue-job-trigger)
  ├─ Extract date from filename: YYYYMMDD (regex: (\d{8})\.csv$)
  ├─ Call: glue.start_job_run(JobName=..., Arguments={'--VER_DATE': '20260528'})
  └─ Return: JobRunId + extracted verDate
  ↓
AWS Glue Job (PySpark 3.11 on 2× G.2X workers)
  ├─ Parse --VER_DATE argument: '20260528'
  ├─ Read CSV from S3 raw bucket (wildcard: *.csv)
  ├─ Schema inference (inferSchema=true)
  ├─ Validate required columns + add missing ones
  ├─ Convert date format: 20260528 → 2026-05-28
  ├─ Add metadata columns:
  │   ├─ processed_at: timestamp
  │   ├─ glue_job_run_id: job identifier
  │   └─ ver_date: YYYY-MM-DD (from filename) ✅ NEW Phase 1
  └─ Write Parquet (snappy compression) with Job Bookmark (incremental)
  ↓
S3 Processed Bucket
  ├─ processed_data/part-00000-*.snappy.parquet
  ├─ processed_data/part-00001-*.snappy.parquet
  └─ spark-logs/

Scheduled Option (via EventBridge):
  6:00 AM UTC → EventBridge Cron Rule → Lambda (optional alternate trigger)

Status: ✅ Phase 1 Complete - Date-based file processing OPERATIONAL
```

---

### Phase 1 Implementation Details

**File Naming Convention:**

- Input: `sns_advertisement_YYYYMMDD.csv` (e.g., `sns_advertisement_20260528.csv`)
- Lambda extracts 8-digit date via regex: `(\d{8})\.csv$`
- Passes to Glue as: `--VER_DATE 20260528`
- Glue converts to: `2026-05-28` and adds as `ver_date` column

**Output Schema:**

```
video_title: string
views: int32
channel_name: string
channel_subscribers: int32
likes: int32
video_duration_minutes: int32
processed_at: string (ISO timestamp)
glue_job_run_id: string
ver_date: string (YYYY-MM-DD format) ✅
```

**Example Output (Parquet):**

```
Row 1: title=cute_cats_001, views=50000, ..., ver_date=2026-05-28
Row 2: title=gaming_001, views=120000, ..., ver_date=2026-05-28
```

**S3 Structure:**

```
Raw Bucket (Input):
  s3://dev-karasuit-raw-bucket/
  ├─ sns_advertisement_20260528.csv
  ├─ sns_advertisement_20260527.csv
  └─ [files at bucket root - no /raw/ prefix]

Processed Bucket (Output):
  s3://dev-karasuit-processed-bucket/
  ├─ processed_data/
  │  ├─ part-00000-*.snappy.parquet
  │  ├─ part-00001-*.snappy.parquet
  │  └─ [partitions by Spark parallelization]
  └─ [output at bucket root with /processed_data/ subfolder]
```

**Trigger Methods:**

1. **S3 Event (Manual)**: Upload CSV → Lambda automatically triggered
2. **EventBridge (Scheduled)**: Daily 6 AM UTC (if enabled)
3. **Direct AWS Console**: glue.start_job_run() manually

---

## Technical Stack

| Component                | Purpose                                      | Status |
| ------------------------ | -------------------------------------------- | ------ |
| **AWS Glue 4.0**         | PySpark ETL (2× G.2X workers)                | ✅     |
| **Lambda (Python 3.12)** | Date extraction + job trigger via Boto3      | ✅     |
| **EventBridge**          | Optional: daily cron schedule (6 AM UTC)     | ✅     |
| **S3 (2 buckets)**       | Raw CSV input + Parquet output (no prefixes) | ✅     |
| **Parquet (snappy)**     | Columnar format with compression             | ✅     |
| **Job Bookmark**         | Incremental processing state                 | ✅     |
| **Terraform**            | Infrastructure as Code for all AWS resources | ✅     |
| **Date Processing**      | YYYYMMDD → YYYY-MM-DD conversion (Phase 1)   | ✅     |
| **Apache Iceberg**       | Table format (future enhancement)            | 🔄     |
| **Claude API**           | Schema automation (future enhancement)       | 🔄     |

---

## Why This Architecture

| Decision                          | Rationale                                   | Trade-off                                    |
| --------------------------------- | ------------------------------------------- | -------------------------------------------- |
| **AWS Glue**                      | Managed PySpark, no cluster ops             | Higher cost than self-managed Spark          |
| **Lambda + S3 PUT Event**         | Serverless, event-driven (not poll-based)   | Not compatible with EventBridge (for now)    |
| **Date in Filename + Extraction** | Deterministic versioning per data snapshot  | Requires strict naming convention (YYYYMMDD) |
| **Parquet**                       | Efficient storage, industry-standard        | Manual schema evolution (future: Claude API) |
| **Terraform**                     | Infrastructure reproducibility, version IaC | Steep learning curve for beginners           |
| **2-Worker Glue**                 | Balanced cost/throughput for production     | Overkill for sample data (but realistic)     |
| **Job Bookmark**                  | Incremental processing, idempotent runs     | Requires careful state management            |
| **Wildcard CSV Pattern**          | Flexible multi-file batching per run        | Must handle edge cases (empty dirs, naming)  |

---

## Architecture Decisions

| Phase | Feature                     | Implementation                           | Trade-offs                               |
| ----- | --------------------------- | ---------------------------------------- | ---------------------------------------- |
| 0.5   | **Basic ETL**               | Glue + Lambda + S3 + Terraform           | Manual schema management needed          |
| 1     | **Date-Based Processing**   | Filename regex + --VER_DATE + conversion | Strict naming required (YYYYMMDD format) |
| 2     | **Schema Automation (TBD)** | Claude API for code generation           | API costs, latency, requires monitoring  |
| 3     | **Iceberg Migration (TBD)** | Time-travel + ACID support               | Higher complexity, schema evolution      |

````

---

## What's Included

### Current Implementation ✅

**Phase 0.5 (Base ETL):**
- AWS Glue ETL job (PySpark) with schema inference
- Lambda trigger function (Python 3.12)
- EventBridge daily cron schedule (6 AM UTC, optional)
- S3 data lake (raw bucket + processed bucket, no prefixes)
- Terraform modules for reproducible deployment
- End-to-end validation (CSV → Parquet working)

**Phase 1 (Date-Based Processing) - COMPLETE:**
- ✅ Date extraction from CSV filename (regex: `(\d{8})\.csv$`)
- ✅ Lambda passes `--VER_DATE` argument to Glue (format: YYYYMMDD)
- ✅ Glue converts date: YYYYMMDD → YYYY-MM-DD
- ✅ `ver_date` column added to Parquet output
- ✅ Job Bookmark enabled for incremental processing
- ✅ Wildcard CSV pattern (`*.csv`) for multi-file batching
- ✅ Multi-file test: 2 runs, 19 total rows with correct dates

### Potential Enhancements

**Phase 2 (Schema Automation):**
- Claude API integration to auto-generate Glue code changes
- Detect schema changes and trigger code updates
- Auto-test generated transformations

**Phase 3 (Apache Iceberg):**
- Migrate from Parquet to Iceberg tables
- Enable time-travel queries
- ACID compliance for concurrent writes
- Schema evolution built-in

**Monitoring & Operations:**
- CloudWatch dashboards (Glue execution metrics)
- Data quality checks (row count, schema validation)
- Error alerting (SNS notifications)
- Cost optimization (reserved capacity analysis)

---

## Getting Started

### Prerequisites

- AWS Account with credentials configured
- Terraform >= 1.0
- AWS CLI v2

### Quick Start

```bash
# Clone and deploy
git clone https://github.com/Karasu1t/claude-driven-schema-evolution.git
cd claude-driven-schema-evolution

# Deploy infrastructure
cd terraform/env/dev/aws
terraform init
terraform plan
terraform apply

# Upload CSV and trigger Glue Job
aws s3 cp <local_csv_path> s3://<raw-bucket>/raw/
aws glue start-job-run --job-name <glue-job-name> --region <region>

# Check output
aws s3 ls s3://<processed-bucket>/
```

---

## Project Structure

```
claude-driven-schema-evolution/
├── README.md (this file)
├── docs/
│   └── PHASE_0.5_AWS_DESIGN.md (architecture details)
├── terraform/
│   ├── modules/
│   │   └── aws/
│   │       ├── glue_job/              (Glue ETL + script upload)
│   │       ├── lambda_trigger/        (Python 3.12 date extractor)
│   │       ├── eventbridge/           (optional daily schedule)
│   │       ├── s3_raw_bucket/         (input CSV location)
│   │       └── s3_processed_bucket/   (Parquet output location)
│   └── env/
│       └── dev/aws/                   (dev environment config)
├── src/
│   └── glue/
│       └── glue_job.py                (PySpark + ver_date logic)
├── terraform/modules/aws/lambda_trigger/
│   └── index.py                       (date extraction regex)
├── data/
│   ├── sns_advertisement_20260528.csv (11 rows sample - Phase 1 test)
│   └── sns_advertisement_20260527.csv (8 rows sample - Phase 1 test)
└── specs/
    └── schema_v1.yaml                 (schema definition)
```

---

## Testing

### Phase 1 Test Results (2026-05-28)

#### Test Case 1: Single File Processing
```
Input:  sns_advertisement_20260528.csv (11 rows)
Lambda: Date extraction → '20260528' ✅
Glue:   SUCCEEDED (45 sec execution time)
Output:
  - 11 rows with ver_date = 2026-05-28 ✅
  - Schema validation: 6 original + 3 metadata columns ✅
  - Parquet file: 3.6 KB (snappy compression)
```

#### Test Case 2: Multi-File Processing (Incremental)
```
Input:
  - sns_advertisement_20260528.csv (11 rows, first run)
  - sns_advertisement_20260527.csv (8 rows, second run)

Lambda Runs:
  Run 1: verDate=20260528 ✅
  Run 2: verDate=20260527 ✅

Glue Runs:
  Run 1: SUCCEEDED (45 sec) → 11 rows with ver_date=2026-05-28
  Run 2: SUCCEEDED (77 sec) → 19 rows (11+8) with ver_date=2026-05-27

Output Files:
  - part-00000-...-c000.snappy.parquet (11 rows)
  - part-00001-...-c000.snappy.parquet (8 rows)

Job Bookmark: ✅ Incremental processing verified
```

**Verification:**
```bash
# Parquet inspection (via pyarrow)
Columns: [video_title, views, channel_name, channel_subscribers, likes, video_duration_minutes, processed_at, glue_job_run_id, ver_date]
Row Count: 11 (first) + 19 (second cumulative, includes bookmark)
ver_date Format: YYYY-MM-DD (conversion working ✅)
```

### Phase 0.5 Test Results (Previous)

```
✅ Glue Job: SUCCEEDED (Schema inference + metadata)
✅ Lambda Trigger: SUCCEEDED (Python 3.12 runtime)
✅ EventBridge Schedule: ENABLED (daily 6 AM UTC)
✅ S3 Buckets: Both operational (versioning + encryption)
```

---

## Cost Estimation (Monthly)

| Service     | Cost      | Notes            |
| ----------- | --------- | ---------------- |
| Glue        | $0.75     | 1 DPU-hour/day   |
| Lambda      | <$0.01    | 1 invocation/day |
| S3          | <$1       | Sample data      |
| EventBridge | <$0.01    | Free tier        |
| **Total**   | **~$2-3** | Production-ready |

---

## License

MIT License - see LICENSE file
````
