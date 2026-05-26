# Claude-Driven Schema Evolution for Data Engineering

**AI-Assisted ETL with Schema Evolution on AWS**

A portfolio project demonstrating cloud-native data engineering practices: CSV schema changes → AWS Glue transformation → Parquet output. Phase 0.5 establishes a complete, production-ready ETL pipeline on AWS. Phase 1 adds Claude API integration for automatic schema evolution handling. Phase 2 migrates to Apache Iceberg for advanced data governance.

**Target Audience:** Data engineers applying for EU/Switzerland 2028 roles seeking hands-on cloud architecture + AI integration proof.

**Repository:** [`claude-driven-schema-evolution`](https://github.com/Karasu1t/claude-driven-schema-evolution)

---

## 日本語: プロジェクト概要

**背景・課題:**

ビデオプラットフォームが提供するデータフィードの CSV スキーマは、頻繁に変更される（新カラム追加など）。
従来の対応プロセス：

1. CSV スキーマ変更を検出（手動）
2. PySpark 変換コードを修正（手動）
3. ローカルテスト実施
4. AWS Glue Job を再デプロイ

**問題点：**

- スキーマ変更のたびに手動コード修正が発生
- テスト・検証が属人化している
- デプロイプロセスに時間がかかる
- スキーマ進化への対応が遅い

**ソリューション:**

本プロジェクトは 3 フェーズで構成：

**Phase 0.5 (✅ 完了)**: AWS Glue ベースの本番パイプライン確立

- EventBridge でスケジュール実行
- Lambda で Glue Job トリガー
- S3 (Parquet) へ自動出力
- 完全に動作するシステム完成

**Phase 1 (次)**: スキーマ進化対応システム構築

- 新カラム追加に対応した PySpark ロジック実装
- Glue Job を動的スキーマ対応に拡張
- E2E テストで検証

**Phase 2**: Claude API 統合（運用自動化）

- Claude でスキーマ変更を自動検出
- PySpark 変換コード自動生成
- 検証フロー自動化

**技術スタック (Phase 0.5 完了時点)：**

- **AWS Glue**: PySpark ベースの ETL 実行エンジン
- **AWS Lambda**: スケジューラー (EventBridge) と Glue Job の仲介層
- **AWS EventBridge**: 毎日 6 AM UTC の自動トリガー
- **AWS S3**: データレイク (raw bucket + processed bucket)
- **Parquet**: 列指向データフォーマット（中期）
- **Apache Iceberg**: テーブルフォーマット（Phase 2）
- **Terraform**: Infrastructure as Code（全 AWS リソース）

---

## What This Does (Phase 0.5 - Live)

```
Daily Workflow (自動実行):

6:00 AM UTC
  ↓
EventBridge Cron Rule トリガー
  ↓
Lambda Function (dev-karasuit-glue-job-trigger)
  ├─ Glue Job 起動リクエスト
  └─ return JobRunId
  ↓
AWS Glue Job (dev-karasuit-schema-evolution-etl)
  ├─ S3 raw bucket から CSV 読み込み
  ├─ スキーマ推測 (inferSchema=true)
  ├─ 必須カラム検証 + 追加（足りない場合）
  ├─ メタデータ添付 (processed_at, glue_job_run_id)
  └─ Parquet 書き込み (snappy 圧縮)
  ↓
S3 processed bucket
  ├─ part-00000-*.snappy.parquet (Worker #0)
  ├─ part-00001-*.snappy.parquet (Worker #1)
  └─ spark-logs/ (実行ログ)

Status: ✅ All SUCCEEDED
```

---

## Technical Stack (Phase 0.5)

| Component                           | Purpose                               | Status     |
| ----------------------------------- | ------------------------------------- | ---------- |
| **AWS Glue 4.0**                    | PySpark ETL execution engine          | ✅ LIVE    |
| **Lambda (Python 3.12)**            | Glue Job trigger via Boto3            | ✅ LIVE    |
| **EventBridge (CloudWatch Events)** | Daily cron schedule (6 AM UTC)        | ✅ LIVE    |
| **S3 (2 buckets)**                  | Raw CSV input + Parquet output        | ✅ LIVE    |
| **Parquet (snappy)**                | Columnar data format with compression | ✅ LIVE    |
| **Terraform**                       | IaC for all AWS resources             | ✅ LIVE    |
| **Apache Iceberg**                  | Table format for Phase 2              | 🔄 Planned |
| **Claude API**                      | Schema evolution automation (Phase 2) | 🔄 Planned |

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

## Phase Roadmap

### Phase 0.5 ✅ (COMPLETE)

**Deliverables:**

- ✅ AWS Glue ETL pipeline running daily
- ✅ Lambda auto-triggers Glue Job
- ✅ CSV → Parquet transformation verified
- ✅ S3 data lake structure in place
- ✅ Terraform IaC for all resources
- ✅ End-to-end test passed (100 rows)

**Output:** Fully operational, production-ready data pipeline

---

### Phase 1 (NEXT - Schema Evolution System)

**Goal:** Build complete schema evolution handling without Claude

**Deliverables:**

- New CSV with 8 columns (subscriber_delta, dislikes added)
- Updated Glue Job with dynamic column handling
- E2E test: verify new columns processed correctly
- Terraform updated
- Git commit with proof

**Output:** System that adapts to schema changes (manual code update)

---

### Phase 2 (FUTURE - Claude Automation)

**Goal:** Automate Phase 1 with Claude API

**Deliverables:**

- Claude API integration for schema change detection
- Auto-generate PySpark transformation code
- SKILL command: `claude fix-etl --schema <changes.json>`
- Automated validation workflow
- AWS deployment automation

**Output:** One-command schema evolution handling

---

### Phase 3 (FUTURE - Iceberg Migration)

**Goal:** Upgrade to Apache Iceberg for enterprise features

**Deliverables:**

- Migration: Parquet → Iceberg tables
- Glue Catalog metadata centralization
- Time Travel support (point-in-time queries)
- ACID transaction handling

**Output:** Enterprise-grade data governance
Gate -->|"❌ Failed"| FixNeeded["Fix & Re-push"]

```

---

## Scope

### Phase 0.5 Implemented ✅

- ✅ AWS Glue ETL pipeline (PySpark-based)
- ✅ Lambda trigger integration
- ✅ EventBridge cron scheduling
- ✅ S3 data lake structure
- ✅ Terraform IaC for all resources
- ✅ End-to-end CSV → Parquet pipeline

### Phase 1 Planned (Schema Evolution)

- Glue Job with dynamic column handling
- 8-column CSV processing
- E2E testing framework

### Phase 2 Planned (Claude Integration)

- Claude API for schema change detection
- Auto-code generation
- Validation workflows

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

| Service | Cost | Notes |
|---------|------|-------|
| Glue | $0.75 | 1 DPU-hour/day |
| Lambda | <$0.01 | 1 invocation/day |
| S3 | <$1 | Sample data |
| EventBridge | <$0.01 | Free tier |
| **Total** | **~$2-3** | Production-ready |

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

---

## Contact

**Author:** Karasuit  
**Target:** 2028 EU/Switzerland data engineering roles

**Key Evidence:**
- Production AWS architecture (Glue + Lambda + EventBridge)
- Infrastructure as Code (Terraform)
- Hands-on cloud data engineering
- AI integration roadmap (Phase 2: Claude API)
