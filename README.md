# Claude-Driven Schema Evolution for Data Engineering

**Iceberg-First ETL Pipeline on AWS Glue with Test/Production Separation**

An enterprise data engineering pipeline: CSV (evolving schemas) → AWS Glue (PySpark) → Apache Iceberg (ACID, time-travel, versioning).
- **Format**: Apache Iceberg v2 (mandatory, no fallback)
- **Catalog**: AWS Glue Catalog (GlueCatalog)
- **Environment Isolation**: Separate test and production buckets with automatic detection
- **Orchestration**: EventBridge scheduled trigger (6:00 AM UTC daily)

---

## 🎯 Project Overview

### Background

Video platform data feeds arrive in CSV format with frequent schema changes. This project implements **automated schema evolution** with complete test/production separation.

✅ **Automated Daily ETL**: EventBridge → Lambda → Glue Job (6:00 AM UTC)  
✅ **Schema Flexibility**: Dynamic column detection and handling  
✅ **Iceberg Format**: Time-travel queries, ACID transactions, version history  
✅ **Test/Prod Isolation**: Separate S3 buckets with automatic switching  
✅ **Infrastructure as Code**: Terraform for reproducible deployments  

---

## 🚀 Pipeline Architecture

### Data Flow

```
CSV File (with date in filename)
    ↓
S3 Upload (dev-karasuit-raw-bucket or dev-karasuit-test-raw-bucket)
    ↓
Lambda Trigger (extracts YYYYMMDD from filename)
    ├─ Detects environment: is filename in 'test' bucket?
    ├─ Sets INPUT_BUCKET & OUTPUT_BUCKET accordingly
    └─ Invokes Glue Job with --VER_DATE argument
    ↓
AWS Glue Job (PySpark 3.11)
    ├─ Reads CSV from dynamic INPUT_BUCKET
    ├─ Schema inference + mandatory columns check
    ├─ Iceberg catalog configuration (GlueCatalog)
    ├─ Creates test/prod database (if not exists)
    ├─ Writes to Iceberg with partitioning by date
    └─ Outputs to dynamic OUTPUT_BUCKET
    ↓
S3 Iceberg Warehouse
    ├─ Test: s3://dev-karasuit-test-processed-bucket/iceberg-warehouse/
    └─ Prod: s3://dev-karasuit-processed-bucket/iceberg-warehouse/
    ↓
AWS Glue Catalog + Athena
    └─ Query: SELECT COUNT(*) FROM video_advertisement;
```

### File Naming Convention

**Required Format:** `sns_advertisement_YYYYMMDD.csv`

Example: `sns_advertisement_20260529.csv`

Lambda extracts the date via regex: `(\d{8})\.csv$` → `20260529`

---

## ✅ Verification Status (May 29, 2026)

### End-to-End Pipeline Testing

| Phase                          | Test Env | Prod Env | Status |
|--------------------------------|----------|----------|--------|
| **CSV Upload**                 | ✅       | ✅       | Tested |
| **Lambda Trigger**             | ✅       | ✅       | Verified - Auto-detection working |
| **Glue Job Execution**         | ✅       | ✅       | SUCCEEDED (55-99 sec) |
| **CSV Read (5 rows)**          | ✅       | ✅       | Confirmed |
| **Iceberg Table Creation**     | ✅       | ✅       | GlueCatalog registered |
| **Data Write with Partition**  | ✅       | ✅       | Verified - partition_date=2026-05-29 |
| **Athena Query**               | ✅       | ✅       | SELECT returned 5 rows |

### Test Data Example

```csv
video_title,views,channel_name,channel_subscribers,likes,video_duration_minutes
Python Tutorial,45000,CodeMastery,120000,2300,42.5
Data Analysis,38000,DataLab,98000,1900,38.0
AWS Basics,52000,CloudTech,145000,2800,45.0
Machine Learning,61000,AIExperts,189000,3500,52.0
Database Design,47000,DataEng,112000,2100,40.0
```

---

## 🔧 Environment Separation

### Automatic Detection

Lambda detects test mode by checking bucket name:

```python
if 'test' in bucket_from_event.lower():
    input_bucket = 'dev-karasuit-test-raw-bucket'
    output_bucket = 'dev-karasuit-test-processed-bucket'
else:
    input_bucket = 'dev-karasuit-raw-bucket'
    output_bucket = 'dev-karasuit-processed-bucket'
```

### Bucket Configuration

**Test Environment:**
- Input: `dev-karasuit-test-raw-bucket`
- Output: `dev-karasuit-test-processed-bucket`
- Glue Database: `dev_karasuit_iceberg_db_test`
- Iceberg Path: `s3://dev-karasuit-test-processed-bucket/iceberg-warehouse/`

**Production Environment:**
- Input: `dev-karasuit-raw-bucket`
- Output: `dev-karasuit-processed-bucket`
- Glue Database: `dev_karasuit_iceberg_db`
- Iceberg Path: `s3://dev-karasuit-processed-bucket/iceberg-warehouse/`

---

## 📁 AWS Infrastructure

### Glue Job Configuration

- **Job Name**: `dev-karasuit-schema-evolution-etl`
- **Runtime**: PySpark 3.11 on AWS Glue 4.0
- **Workers**: 2× G.2X (2 DPU each)
- **Timeout**: 30 minutes
- **Job Bookmark**: DISABLED (for dev - allows re-processing test data)
- **Arguments**: `--JOB_NAME`, `--INPUT_BUCKET`, `--OUTPUT_BUCKET`, `--VER_DATE`

### Lambda Configuration

- **Function Name**: `dev-karasuit-glue-job-trigger`
- **Runtime**: Python 3.12
- **Role**: IAM role with Glue permissions
- **Trigger**: EventBridge scheduled rule
- **Responsibilities**:
  - Extract date from S3 filename
  - Detect test vs. production mode
  - Invoke Glue Job with dynamic arguments

### EventBridge Rule

- **Rule Name**: `dev-karasuit-scheduled-trigger-rule`
- **Schedule**: `cron(0 6 * * ? *)` (Daily 6:00 AM UTC)
- **Target**: Lambda function `dev-karasuit-glue-job-trigger`

---

## 📊 Iceberg Schema

### Required Columns (From CSV)

```
video_title          STRING
views                LONG
channel_name         STRING
channel_subscribers  LONG
likes                LONG
video_duration_minutes DOUBLE
```

### Metadata Columns (Added by Glue)

```
partition_date  DATE      (extracted from filename YYYYMMDD)
processed_at    STRING    (ISO format timestamp)
```

### Partitioning Strategy

- **Partition Column**: `partition_date`
- **Format**: Extracted from filename (YYYYMMDD → YYYY-MM-DD)
- **Benefit**: Optimizes queries on specific dates, reduces scan time

---

## 🛠️ Deployment & Terraform

### Deploy Infrastructure

```bash
cd terraform/env/dev/aws
terraform init
terraform plan
terraform apply
```

### Key Terraform Modules

- **S3 Buckets**: Raw and processed buckets (test + prod)
- **Glue Job**: PySpark ETL job with Iceberg configuration
- **Lambda Function**: Trigger function with date extraction
- **IAM Roles & Policies**: Permissions for Glue, Lambda, S3
- **EventBridge Rule**: Scheduled daily invocation

---

## 🧪 Manual Testing

### Test CSV Upload

```bash
# Upload to test bucket
aws s3 cp test_data/sns_advertisement_20260529.csv \
  s3://dev-karasuit-test-raw-bucket/sns_advertisement_20260529.csv \
  --region ap-northeast-1

# Lambda will auto-invoke on schedule (6 AM UTC) or manually:
aws lambda invoke \
  --function-name dev-karasuit-glue-job-trigger \
  --payload '{"Records":[{"s3":{"bucket":{"name":"dev-karasuit-test-raw-bucket"}}}]}' \
  response.json \
  --region ap-northeast-1
```

### Verify Data in Athena

```sql
-- Check test environment
SELECT COUNT(*) as row_count, partition_date 
FROM dev_karasuit_iceberg_db_test.video_advertisement 
GROUP BY partition_date;

-- Expected output: 5 rows, partition_date=2026-05-29
```

---

## 📝 Schema Evolution Handling

### Current Approach

1. **Defined Schema**: Hardcoded in Glue Job for reliability
2. **Missing Columns**: Added dynamically with `withColumn(col, lit(None))`
3. **Data Type Mismatch**: Handled by PERMISSIVE mode + explicit casting

### Adding New Columns

To support new CSV columns:

1. Add to schema definition in `src/glue/glue_job.py`
2. Update Iceberg table (new version created automatically)
3. Redeploy Glue Job: `terraform apply -target=module.glue_etl_job`

---

## 🔍 Debugging & Logs

### CloudWatch Logs

```bash
# Tail Glue Job logs
aws logs tail /aws-glue/jobs/output \
  --follow=false \
  --region ap-northeast-1

# Filter by Job ID
aws logs tail /aws-glue/jobs/output \
  --filter-pattern "jr_ca3383cb1e7bd7faa2313e3e7d4bcedc9863c88179f90116c7b19e415c960571" \
  --region ap-northeast-1
```

### Check Job Status

```bash
aws glue get-job-run \
  --job-name dev-karasuit-schema-evolution-etl \
  --run-id <JOB_RUN_ID> \
  --region ap-northeast-1
```

---

## 📦 Project Structure

```
.
├── src/
│   └── glue/
│       └── glue_job.py              # PySpark ETL script
├── terraform/
│   ├── modules/aws/
│   │   ├── s3/                      # S3 bucket modules (raw, processed, test)
│   │   ├── glue_job/                # Glue Job configuration
│   │   ├── lambda_trigger/          # Lambda function for date extraction
│   │   ├── eventbridge/             # EventBridge scheduled rule
│   │   └── iam/                     # IAM roles and policies
│   └── env/dev/aws/
│       └── main.tf                  # Dev environment definition
├── test_data/
│   └── sns_advertisement_20260529.csv  # Test dataset
└── README.md                        # This file
```

---

## 🎓 Key Learnings

1. **Filename Patterns Matter**: Spark CSV reader is sensitive to naming conventions
   - ✅ Works: `sns_advertisement_20260529.csv`
   - ❌ Fails: `_20260529.csv` (0 rows returned)

2. **CRLF Line Endings**: CSV files must use LF (Unix), not CRLF (Windows)

3. **Job Bookmark Disabled for Dev**: Allows re-processing test data without manual reset

4. **Automatic Test/Prod Switching**: Environment detection by bucket name reduces error-prone configuration

5. **Iceberg Partitioning**: Date-based partitioning optimizes query performance significantly

---

## 🚀 Next Steps

- [ ] Schedule real production data ingestion (6 AM UTC)
- [ ] Add error alerting (SNS notifications on Glue Job failure)
- [ ] Implement data validation tests (row counts, schema validation)
- [ ] Add Iceberg time-travel query examples
- [ ] Document rollback procedures

---

## 📝 Version History

**May 29, 2026 - Phase 5 Complete**
- ✅ Iceberg v2 mandatory format (no Parquet fallback)
- ✅ GlueCatalog integration
- ✅ Test/Production environment separation
- ✅ Multi-pattern filename detection
- ✅ End-to-end pipeline verified and tested

