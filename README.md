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

## How It Works (Current Implementation)

```
Automated Daily Workflow:

6:00 AM UTC
  ↓
EventBridge Cron Rule
  ↓
Lambda Function (dev-karasuit-glue-job-trigger)
  ├─ Calls: glue.start_job_run(JobName="dev-karasuit-schema-evolution-etl")
  └─ Returns: JobRunId
  ↓
AWS Glue Job (PySpark 3.11 on 2× G.2X workers)
  ├─ Reads CSV from S3 raw bucket
  ├─ Schema inference (inferSchema=true)
  ├─ Validates required columns + adds missing ones
  ├─ Adds metadata (processed_at, glue_job_run_id)
  └─ Writes Parquet (snappy compression)
  ↓
S3 Processed Bucket
  ├─ part-00000-*.snappy.parquet
  ├─ part-00001-*.snappy.parquet
  └─ spark-logs/

Status: ✅ All Components LIVE and OPERATIONAL
```

---

## Technical Stack

| Component                | Purpose                                 | Status |
| ------------------------ | --------------------------------------- | ------ |
| **AWS Glue 4.0**         | PySpark ETL execution (2× G.2X workers) | ✅     |
| **Lambda (Python 3.12)** | Job trigger via Boto3                   | ✅     |
| **EventBridge**          | Daily cron schedule (6 AM UTC)          | ✅     |
| **S3 (2 buckets)**       | Raw CSV input + Parquet output          | ✅     |
| **Parquet (snappy)**     | Columnar format with compression        | ✅     |
| **Terraform**            | Infrastructure as Code for all AWS      | ✅     |
| **Apache Iceberg**       | Table format (future enhancement)       | 🔄     |
| **Claude API**           | Schema automation (future enhancement)  | 🔄     |

---

## Why This Architecture

| Decision                 | Rationale                                       | Trade-off                                      |
| ------------------------ | ----------------------------------------------- | ---------------------------------------------- |
| **AWS Glue**             | Managed PySpark, no cluster ops                 | Higher cost than self-managed Spark            |
| **Lambda + EventBridge** | Serverless, reliable scheduling                 | Not stream-driven (daily batch only)           |
| **Parquet**              | Efficient storage, widespread tooling           | Manual schema evolution (Phase 1 solves)       |
| **Terraform**            | Infrastructure reproducibility, version control | Steep learning curve for beginners             |
| **2-Worker Glue**        | Balanced cost/throughput for prototype          | Overkill for 100-row sample (production-ready) |

---

## Architecture Decisions

| Decision                 | Implementation                              | Trade-offs                              |
| ------------------------ | ------------------------------------------- | --------------------------------------- |
| **AWS Glue**             | Managed PySpark ETL                         | Managed service cost vs self-hosted     |
| **Lambda + EventBridge** | Serverless scheduling + job trigger         | No stream-driven processing (batch only) |
| **Parquet**              | Columnar format with compression            | Manual schema evolution needed (v1.0)  |
| **Terraform**            | Infrastructure as Code                      | Learning curve, but reproducibility    |
| **2-Worker Cluster**     | Balanced cost/throughput for production     | Overkill for sample data, but realistic |
Gate -->|"❌ Failed"| FixNeeded["Fix & Re-push"]

````

---

## What's Included

### Current Implementation ✅

- AWS Glue ETL job (PySpark) with schema inference
- Lambda trigger function (Python 3.12)
- EventBridge daily cron schedule (6 AM UTC, adjustable)
- S3 data lake (raw bucket + processed bucket)
- Terraform modules for reproducible deployment
- End-to-end validation (CSV → Parquet working)

### Potential Enhancements

- **Schema Evolution v2**: Handle additional columns dynamically (code ready, test pending)
- **Claude Integration**: Auto-generate code changes for schema updates (API ready)
- **Apache Iceberg**: Upgrade from Parquet for time-travel + ACID support
- **Data Quality Checks**: Schema validation, row count verification, data profiling

---

## Getting Started

### Prerequisites

- AWS Account with credentials configured
- Terraform >= 1.0
- AWS CLI v2

### Quick Start

```bash
# 1. Clone
git clone https://github.com/Karasu1t/claude-driven-schema-evolution.git
cd claude-driven-schema-evolution

# 2. Deploy infrastructure
cd terraform/env/dev/aws
terraform init
terraform plan
terraform apply

# 3. Upload sample data
aws s3 cp ../../data/sample_input.csv s3://dev-karasuit-raw-bucket/raw/

# 4. Trigger Glue Job
aws glue start-job-run --job-name dev-karasuit-schema-evolution-etl --region ap-northeast-1

# 5. Check output
aws s3 ls s3://dev-karasuit-processed-bucket/processed/ --recursive
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
│   │       ├── glue_job/
│   │       ├── lambda_trigger/
│   │       ├── eventbridge/
│   │       ├── s3_raw_bucket/
│   │       └── s3_processed_bucket/
│   └── env/
│       └── dev/aws/
├── src/
│   └── glue/
│       └── glue_job.py
├── data/
│   └── sample_input.csv
└── specs/
    └── schema_v1.yaml
```

---

## Testing

### Phase 0.5 Test Results

```
✅ Glue Job: SUCCEEDED
✅ Lambda Trigger: SUCCEEDED
✅ EventBridge Schedule: ENABLED
✅ CSV → Parquet: VERIFIED
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

## Troubleshooting

### Glue Job Permission Error

```bash
aws iam get-role-policy \
  --role-name dev-karasuit-glue-job-role \
  --policy-name dev-karasuit-glue-job-policy
```

### CSV Not Found

```bash
aws s3 cp data/sample_input.csv s3://dev-karasuit-raw-bucket/raw/
```

---

## License

MIT License - see LICENSE file
