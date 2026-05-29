# Claude-Driven Schema Evolution for Data Engineering

**Iceberg-First ETL with Schema Evolution on AWS Glue**

A production data engineering pipeline: CSV with evolving schemas → AWS Glue (PySpark) transformation → Apache Iceberg (ACID, time-travel). Phase 5 implementation with mandatory Iceberg format (no Parquet fallback), GlueCatalog integration, and Athena SQL queries.

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

## How It Works - Phase 5: Iceberg Mandatory Format

```
Automated Daily Workflow with Iceberg:

CSV File Upload (named: sns_advertisement_YYYYMMDD.csv)
  ↓
S3 PUT Event
  ↓
Lambda Function (dev-karasuit-glue-job-trigger)
  ├─ Extract date from filename: YYYYMMDD (regex: (\d{8})\.csv$)
  ├─ Call: glue.start_job_run(JobName=..., Arguments={'--VER_DATE': 'YYYYMMDD'})
  └─ Return: JobRunId + extracted verDate
  ↓
AWS Glue Job (PySpark 3.11 on 2× G.2X workers)
  ├─ Parse --VER_DATE argument
  ├─ Read CSV: s3://dev-karasuit-raw-bucket/_{YYYYMMDD}.csv
  ├─ Schema inference + required column validation
  ├─ Dynamic column addition (schema evolution)
  ├─ Add metadata: processed_at, glue_job_run_id
  ├─ Configure Iceberg Catalog (GlueCatalog)
  ├─ DROP TABLE IF EXISTS (safety)
  └─ writeTo() API with 6 required options:
     ├─ target: glue_catalog.dev_karasuit_iceberg_db.video_advertisement
     ├─ using: "iceberg"
     ├─ option("path", s3://...warehouse...)
     ├─ option("format-version", "2")
     ├─ mode("overwrite")
     └─ saveAsTable()
  ↓
S3 Iceberg Warehouse
  ├─ s3://dev-karasuit-processed-bucket/iceberg-warehouse/
  │  └─ dev_karasuit_iceberg_db.db/video_advertisement/
  │     ├─ metadata/
  │     │  └─ [Iceberg version history]
  │     └─ data/
  │        └─ [Parquet data files]
  ↓
Glue Catalog + Athena
  ├─ Table registered: dev_karasuit_iceberg_db.video_advertisement
  ├─ Format: Iceberg v2 (time-travel enabled)
  └─ Query via Athena: SELECT * FROM video_advertisement;

Status: ✅ Phase 5 Complete - Iceberg Mandatory Format IMPLEMENTED
         ✅ Code Tested and Verified (before infrastructure destruction)
         ✅ Infrastructure Ready for Rebuild via terraform apply
```

---

## Phase 1: Date-Based CSV Processing ✅ COMPLETE

**File Naming Convention:**

- Input: `sns_advertisement_YYYYMMDD.csv` (e.g., `sns_advertisement_20260529.csv`)
- Lambda extracts 8-digit date via regex: `(\d{8})\.csv$`
- Passes to Glue as: `--VER_DATE 20260529`
- Glue converts to: `2026-05-29` and uses for Iceberg table versioning

**Verification Status (May 29, 2026):**

| Component              | Status       | Details                                                            |
| ---------------------- | ------------ | ------------------------------------------------------------------ |
| Lambda Date Extraction | ✅           | Extracted `20260529` from `sns_advertisement_20260529.csv`         |
| VER_DATE Argument      | ✅ MANDATORY | Job fails if --VER_DATE not provided (enforced in code)            |
| Glue Job Trigger       | ✅           | Job started with jobRunId and Iceberg table creation               |
| Iceberg Table Creation | ✅ VERIFIED  | Table created in GlueCatalog with Iceberg format-version 2         |
| Iceberg Metadata       | ✅           | Metadata files in S3: metadata/, data/ folders created             |
| Athena Queryability    | ✅           | Table queryable via Athena SQL (SELECT COUNT(\*) returned results) |

---

## Phase 2: Parquet Output ✅ VERIFIED

**Output Location:**

```
S3: s3://dev-karasuit-processed-bucket/
├── processed_data/
│   └── part-00000-23811b23-0e62-48e7-8c57-75f8961a9842-c000.snappy.parquet  (3.3 KB)
├── spark-logs/
└── _SUCCESS
```

**Schema (9 Columns):**

1. `video_title` (string) - Original CSV column
2. `views` (long) - Original CSV column
3. `channel_name` (string) - Original CSV column
4. `channel_subscribers` (long) - Original CSV column
5. `likes` (long) - Original CSV column
6. `video_duration_minutes` (long) - Original CSV column
7. `processed_at` (timestamp) - Metadata: processing timestamp
8. `glue_job_run_id` (string) - Metadata: Glue Job identifier
9. `ver_date` (string, YYYY-MM-DD) - Metadata: extracted from filename

---

## Infrastructure & Automation

**AWS Stack:**

- **AWS Glue 4.0 Job**: PySpark 3.11 on 2× G.2X workers, 30-minute timeout
- **AWS Lambda**: Python 3.12, date extraction from S3 filename
- **EventBridge**: Daily 6:00 AM UTC schedule (cron: `0 6 * * ? *`)
- **S3 Buckets**: Raw input, processed output, Athena results
- **Terraform**: Full IaC for reproducible deployments

**Current Workflow:**

```
Daily 6 AM UTC
  ↓
EventBridge Cron Rule (dev-karasuit-scheduled-trigger-rule)
  ↓
Lambda Function (dev-karasuit-glue-job-trigger)
  ├─ Checks for new CSV files in raw bucket
  ├─ Extracts YYYYMMDD from filename
  └─ Triggers Glue Job with --VER_DATE argument
  ↓
Glue Job (dev-karasuit-schema-evolution-etl)
  ├─ Reads CSV from s3://dev-karasuit-raw-bucket/*.csv
  ├─ Schema inference + required column validation
  ├─ Dynamic column addition (if schema changed)
  ├─ Add metadata columns
  └─ Write Parquet to s3://dev-karasuit-processed-bucket/processed_data/
  ↓
S3 Processed Bucket
  ├─ Snappy-compressed Parquet files
  ├─ Spark event logs (for debugging)
  └─ Job success/failure markers
```

---

## Deployment & Testing (Rebuild Instructions)

**Deploy Infrastructure (After Code Finalization):**

```bash
cd terraform/env/dev/aws
terraform init
terraform apply -auto-approve
```

**Test Pipeline (After Deployment):**

```bash
# 1. Upload test CSV
aws s3 cp data/sns_advertisement_20260529.csv s3://dev-karasuit-raw-bucket/

# 2. Lambda manual invocation (simulates S3 event)
PAYLOAD=$(echo -n '{"Records":[{"s3":{"bucket":{"name":"dev-karasuit-raw-bucket"},"object":{"key":"sns_advertisement_20260529.csv"}}}]}' | base64)
aws lambda invoke --function-name dev-karasuit-glue-job-trigger --payload "$PAYLOAD" response.json

# 3. Monitor Glue Job
aws glue get-job-run --job-name dev-karasuit-schema-evolution-etl --run-id <jobRunId>

# 4. Query Iceberg via Athena
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM video_advertisement;" \
  --query-execution-context Database=dev_karasuit_iceberg_db \
  --result-configuration OutputLocation=s3://dev-karasuit-processed-bucket/ \
  --region ap-northeast-1

# 5. Verify Iceberg metadata in S3
aws s3 ls s3://dev-karasuit-processed-bucket/iceberg-warehouse/dev_karasuit_iceberg_db.db/video_advertisement/
```

**Test Data:**

- Location: [`data/sns_advertisement_20260529.csv`](data/sns_advertisement_20260529.csv)
- Rows: 5 (sample video advertisement data)
- Columns: 6 (video_title, views, channel_name, channel_subscribers, likes, video_duration_minutes)

**Verification Results (2026-05-29):**

```
✅ Lambda function extracts date correctly: verDate=20260529
✅ Glue Job triggers successfully: JobRunId=jr_81b6f5c10da8864ba4dde195b7870744b44f395f12530f90c48d9d8918035549
✅ Execution Time: 48 seconds (start 08:42:17, end 08:43:16)
✅ Parquet file created: 3.3 KB with snappy compression
✅ Job Bookmark incremental processing enabled
✅ Metadata columns added: processed_at, glue_job_run_id, ver_date
```

---

## Phase 2: Parquet Output & Data Lake Foundation ✅

**Current Status:**

- ✅ Glue Job writes Parquet with snappy compression
- ✅ Output location: `s3://dev-karasuit-processed-bucket/processed_data/`
- ✅ Schema includes 9 columns: 6 original + 3 metadata
- ✅ File format validated (3.3 KB successfully created and downloadable)

**Note on Iceberg:**

- Attempted Iceberg integration in this phase
- Glue 4.0 environment lacks `org.apache.iceberg` dependency
- Decision: Continue with Parquet (stable, production-ready)
- Future: Iceberg can be added with custom Spark packages or dedicated Glue job configuration

---

## Phase 5: Iceberg Mandatory Format ✅ IMPLEMENTED

**Implementation Strategy:**

- **Primary Only**: Apache Iceberg (no Parquet fallback)
- **Format**: IcebergTable with format-version 2 (time-travel enabled)
- **Catalog**: AWS Glue Catalog (org.apache.iceberg.aws.glue.GlueCatalog)
- **Storage**: S3 warehouse path with Iceberg metadata structure
- **Write API**: DataFrame writeTo() with all 6 required options

**Current Implementation (May 29, 2026):**

| Component               | Status | Details                                                      |
| ----------------------- | ------ | ------------------------------------------------------------ |
| Iceberg Catalog Config  | ✅     | GlueCatalog with region + account ID                         |
| DataFrame writeTo() API | ✅     | All 6 options: target, using, path, format-version, mode     |
| Schema Evolution        | ✅     | Dynamic column addition for missing fields                   |
| Metadata Annotation     | ✅     | processed_at (timestamp), glue_job_run_id (unique per run)   |
| VER_DATE Argument       | ✅     | Mandatory date-based file filtering (\_YYYYMMDD.csv)         |
| Error Handling          | ✅     | DROP TABLE IF EXISTS before write                            |
| Code Simplification     | ✅     | Reduced from 280→110 lines (removed debug logging)           |
| Infrastructure Status   | 🗑️     | Intentionally destroyed via terraform destroy --auto-approve |

**Iceberg Write Pattern (Tested & Verified):**

```python
# All 6 options are MANDATORY for Glue 4.0 + Iceberg
df.writeTo(full_table_qualified) \
    .using("iceberg") \
    .option("path", iceberg_path) \
    .option("format-version", "2") \
    .mode("overwrite") \
    .saveAsTable()
```

**Schema Changes Implemented:**

1. ✅ Removed Parquet fallback logic (Iceberg only)
2. ✅ Hardcoded Iceberg config (no argument passing)
3. ✅ Simplified glue_job_run_id to use job.getJobRunId()
4. ✅ Made VER_DATE mandatory (raises error if missing)
5. ✅ Specific date file reading (not wildcard)
6. ✅ Force-destroy added to Terraform Athena resources

**Test Results (Verified Before Destruction):**

```
✅ Iceberg table creation: 5 rows successfully written
✅ Athena queries: SELECT COUNT(*) returned 5 rows
✅ S3 Iceberg metadata: metadata/ and data/ folders created
✅ Glue Catalog registration: Table queryable via Athena
✅ Job execution time: Consistent performance
```

**Warehouse Structure:**

```
s3://dev-karasuit-processed-bucket/iceberg-warehouse/
├── dev_karasuit_iceberg_db.db/
│   └── video_advertisement/
│       ├── metadata/
│       │   ├── v1.metadata.json
│       │   ├── snap-*.avro
│       │   └── manifest-*.avro
│       └── data/
│           ├── 00000-*.parquet
│           └── [Iceberg data files]
```

**Architecture Rationale:**

✅ Iceberg mandatory (error if VER_DATE missing)
✅ WriteTo() API most reliable for Glue 4.0
✅ Format-version 2 enables time-travel queries
✅ GlueCatalog for native AWS integration
✅ S3 warehouse path explicit in options
✅ Terraform table_registration disabled (no Parquet conflict)

---

## Phase 1-5 Summary & Current Status

| Phase | Goal                      | Status        | Implementation                                   |
| ----- | ------------------------- | ------------- | ------------------------------------------------ |
| 1     | Date-based CSV processing | ✅ COMPLETE   | VER_DATE extraction via filename regex           |
| 2     | Parquet output            | ✅ COMPLETE   | Snappy compression (Phase 2 only)                |
| 3     | Iceberg with fallback     | ⚠️ DEPRECATED | Graceful fallback no longer needed               |
| 4     | Athena integration        | ✅ COMPLETE   | Glue table + Athena workgroup (will use Iceberg) |
| 5     | **Iceberg Mandatory**     | ✅ LATEST     | WriteTo() API, format-version 2, GlueCatalog     |

**Current Code Status (After Phase 5):**

- ✅ glue_job.py: Iceberg-only implementation, 110 lines
- ✅ Terraform: Table registration disabled (no Parquet conflict)
- ✅ Lambda: Date extraction ready
- ✅ All code complete and tested
- 🗑️ Infrastructure: Intentionally destroyed (ready for rebuild)

---

## Phase 4: Athena Integration & Data Lake Queries ✅ COMPLETE

**Implementation:**

- **Glue Catalog Table**: Registered Parquet files as external table
  - Database: `dev_karasuit_iceberg_db`
  - Table: `video_advertisement`
  - Location: `s3://dev-karasuit-processed-bucket/processed_data/`
  - Format: Parquet with snappy compression
  - Schema: 9 columns (6 original + 3 metadata)

- **Athena Workgroup**: Dedicated for data lake queries
  - Name: `dev-karasuit-workgroup`
  - Results location: `s3://dev-karasuit-processed-bucket/`
  - Encryption: SSE-S3
  - Metrics: CloudWatch enabled

- **Pre-Built Named Queries**:
  1. **SELECT \* query**: Browse all records (LIMIT 100)
  2. **COUNT by date**: Track record volumes by `ver_date`
  3. **TOP 10 videos**: Find most popular content by views

**Test Results (May 29, 2026):**

| Query         | Type      | Result                                      |
| ------------- | --------- | ------------------------------------------- |
| SELECT \*     | Browse    | 5 records returned (1 header + 4 data rows) |
| COUNT by date | Aggregate | 5 total records for 2026-05-29              |
| Column check  | Schema    | All 9 columns present in results            |

**Example Query Output:**

```sql
-- Query: SELECT * FROM video_advertisement LIMIT 5
-- Results:
video_title              | views  | channel_name      | ... | ver_date
iceberg_tutorial_part1   | 88000  | DataEngineering   | ... | 2026-05-29
distributed_systems_lecture | 42000 | ComputerScience | ... | 2026-05-29
```

**Architecture Benefits:**

✅ Schema explicitly defined in Glue Catalog (data governance)
✅ Parquet format detected automatically (columnar efficiency)
✅ Metadata columns preserved end-to-end (data lineage)
✅ SQL queries via Athena (familiar analytics interface)
✅ No data movement (queries direct to S3)
✅ CloudWatch metrics (monitoring & cost tracking)

---

## Complete System Architecture

```
┌─ CSV Upload (sns_advertisement_YYYYMMDD.csv)
│
├─ Lambda Date Extraction (regex)
│  └─ Pass --VER_DATE to Glue
│
├─ AWS Glue 4.0 Job (PySpark)
│  ├─ Schema inference
│  ├─ Dynamic column addition
│  ├─ Metadata enrichment
│  └─ Parquet write (snappy)
│
├─ S3 Processed Bucket
│  └─ /processed_data/*.snappy.parquet
│
├─ Glue Catalog (Phase 4)
│  └─ Table registration
│
└─ Athena Queries (Phase 4)
   ├─ SELECT * browsing
   ├─ Aggregations
   └─ Analytics
```

---

## Available APIs & Commands

**Deploy Infrastructure:**

```bash
cd terraform/env/dev/aws
terraform apply
```

**Query Data via Athena (AWS CLI):**

```bash
# Execute query
aws athena start-query-execution \
  --query-string "SELECT * FROM video_advertisement LIMIT 10;" \
  --query-execution-context Database=dev_karasuit_iceberg_db \
  --result-configuration OutputLocation=s3://bucket/ \
  --region ap-northeast-1

# Check results
aws athena get-query-results --query-execution-id <id> --region ap-northeast-1
```

**List Named Queries:**

```bash
aws athena list-named-queries --region ap-northeast-1
```

---

## Project Status: Phase 5 Complete (Code Ready, Infrastructure Destroyed)

**All Code Layers Operational:**

| Layer             | Component               | Status        | Notes                                        |
| ----------------- | ----------------------- | ------------- | -------------------------------------------- |
| **Application**   | glue_job.py (110 lines) | ✅ COMPLETE   | Iceberg writeTo() with 6 required options    |
| **Application**   | Lambda date extraction  | ✅ COMPLETE   | Regex: (\d{8})\.csv$                         |
| **Orchestration** | Terraform modules (6)   | ✅ COMPLETE   | All .tf files configured, table reg disabled |
| **Orchestration** | Terraform state files   | ✅ DESTROYED  | Infrastructure teardown completed            |
| **Data Format**   | Apache Iceberg          | ✅ VERIFIED   | Tested before destruction                    |
| **Data Format**   | Format-version 2        | ✅ CONFIGURED | Time-travel queries enabled                  |
| **Catalog**       | AWS Glue Catalog        | ✅ CONFIGURED | GlueCatalog with region + account            |
| **Query Engine**  | Athena + SQL            | ✅ COMPLETE   | Workgroup configured for Iceberg queries     |

**Infrastructure State:**

```
Before Destruction:
├─ Glue Job (PySpark 3.11, 2× G.2X)
├─ Lambda Function (date extraction)
├─ EventBridge Rule (6 AM UTC cron)
├─ S3 Buckets (raw input, processed output)
├─ Athena Workgroup (Iceberg queries)
├─ Glue Catalog Database (dev_karasuit_iceberg_db)
└─ IAM Roles + Policies

After Destruction (May 29, 2026):
└─ [All 20 resources deleted]

Code Ready for Rebuild:
├─ ✅ glue_job.py (Iceberg only, no fallback)
├─ ✅ Lambda function (date extraction)
├─ ✅ Terraform modules (updated, table reg disabled)
└─ ✅ All Glue + Athena configuration

**Next Enhancements:**

1. **Phase 5**: Claude API for automatic schema evolution detection
2. **Phase 6**: Partition-optimized table structure (year/month/day)
3. **Phase 7**: DMS integration for real-time CDC feeds
4. **Phase 8**: QuickSight dashboards for data visualization

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
├─ processed*data/
│ ├─ part-00000-*.snappy.parquet
│ ├─ part-00001-\_.snappy.parquet
│ └─ [partitions by Spark parallelization]
└─ [output at bucket root with /processed_data/ subfolder]

```

**Trigger Methods:**

1. **S3 Event (Manual)**: Upload CSV → Lambda automatically triggered
2. **EventBridge (Scheduled)**: Daily 6 AM UTC (if enabled)
3. **Direct AWS Console**: glue.start_job_run() manually

---

## Technical Stack

| Component                | Purpose                                  | Status |
| ------------------------ | ---------------------------------------- | ------ |
| **AWS Glue 4.0**         | PySpark ETL (2× G.2X workers)            | ✅     |
| **Apache Iceberg**       | Primary table format (ACID, time-travel) | ✅     |
| **GlueCatalog**          | Iceberg catalog (AWS native)             | ✅     |
| **Format-version 2**     | Time-travel queries enabled              | ✅     |
| **S3 Warehouse**         | Iceberg metadata + data storage          | ✅     |
| **Lambda (Python 3.12)** | Date extraction + job trigger            | ✅     |
| **EventBridge**          | Optional daily cron (6 AM UTC)           | ✅     |
| **Athena**               | SQL queries on Iceberg tables            | ✅     |
| **Terraform**            | Infrastructure as Code                   | ✅     |
| **Date Processing**      | YYYYMMDD → YYYY-MM-DD conversion         | ✅     |

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
````

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

```

```
