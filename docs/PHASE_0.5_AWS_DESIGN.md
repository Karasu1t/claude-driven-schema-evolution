# Phase 0.5: AWS クラウド化 設計書（Glue ベース）

**目的：** ローカルの Python ETL パイプラインを AWS Glue（本番環境）で動作させる

**対象：** CSV → Parquet 変換 + スキーマメタデータ管理 + Iceberg 対応

**期間：** 1-2 週間（実装・テスト込み）

**戦略：** Glue Catalog をメタデータレイヤーとして、Phase 2（Iceberg/Trino）への拡張を想定

---

## 1. アーキテクチャ概要

### Phase 0（ローカル）vs Phase 0.5（AWS Glue）vs Phase 2（Iceberg）

```
PHASE 0（ローカル - 開発）
┌──────────────────────────────────────────────┐
│ specs/sample_columns.csv (ローカル)          │
│         ↓                                    │
│ Python + Polars（手動実行）                  │
│         ↓                                    │
│ data/sample.parquet                          │
│ data/schema_metadata.sqlite                  │
└──────────────────────────────────────────────┘

PHASE 0.5（AWS Glue - 本番志向）
┌────────────────────────────────────────────────────────────┐
│ S3 Data Lake                                               │
│  ├─ s3://etl-xxx/raw/input.csv                             │
│  ├─ s3://etl-xxx/processed/output.parquet                  │
│  └─ s3://etl-xxx/metadata/                                 │
│         ↓ (S3 Event Trigger)                               │
│ AWS Glue Job (PySpark)                                     │
│         ↓                                                  │
│ AWS Glue Catalog ← メタデータ自動登録                      │
│  └─ Table: streaming_etl_schema                            │
│         ↓                                                  │
│ S3 Parquet Output + Glue Catalog Integration               │
└────────────────────────────────────────────────────────────┘

PHASE 2（Iceberg + Trino - 分析レディ）
┌────────────────────────────────────────────────────────────┐
│ Iceberg Tables（Apache Iceberg）                           │
│  └─ Glue Catalog で管理                                    │
│         ↓                                                  │
│ Trino（SQL クエリエンジン）                                │
│  └─ Iceberg メタデータ読み込み可能                         │
└────────────────────────────────────────────────────────────┘
```

---

## 2. AWS リソース構成

### 2.1 S3 Data Lake

**目的：** 構造化データレイク（Iceberg 対応）

```yaml
Bucket Name: streaming-etl-{account-id}-{region}
Region: us-east-1

Folder Structure: s3://streaming-etl-xxx/
  ├─ raw/
  │  └─ input_*.csv          ← 入力 CSV（複数ファイル対応）
  ├─ processed/
  │  └─ output_v*.parquet    ← 出力 Parquet（バージョニング）
  ├─ metadata/
  │  └─ schemas/             ← Glue Catalog メタデータ
  └─ scripts/
  └─ glue_job.py          ← Glue ETL スクリプト

Versioning:
  Enabled: Yes（メタデータ管理・監査ログ用）

Encryption:
  Default: SSE-S3
  Future: SSE-KMS（本番運用向け）

Lifecycle Policy:
  - raw/: 30 days 後 Glacier 移行
  - processed/: 90 days 後削除（バージョン保持）
```

---

### 2.2 AWS Glue Job

**目的：** ETL 処理実行（スケーラブル PySpark）

```yaml
Job Name: streaming-etl-phase0-transformer

Job Type: Spark（Glue 2.0+）

Runtime:
  Glue Version: 4.0
  Python Version: 3.11

Workers:
  Worker Type: G.2X（開発・テスト）
  Number of Workers: 2（Phase 0.5 では十分）
  Scale for Production: 5-10

DPU (Data Processing Units):
  0.0625 DPU/minute（G.2X 2台 = 1 DPU）
  Cost: $0.44/DPU/hour ≈ $4-5/month（小規模）

Timeout: 30 minutes

Max Retries: 1

Bookmarks:
  Enabled: Yes（インクリメンタル処理対応）

Script Location:
  s3://streaming-etl-xxx/scripts/glue_job.py

IAM Role:
  Policy:
    - s3:GetObject (raw/*)
    - s3:PutObject (processed/*)
    - glue:PutTable
    - glue:UpdateTable
    - glue:GetDatabase
    - glue:GetCatalogImportStatus
```

**Python コード構造：**

```python
# glue_job.py
import sys
import boto3
import polars as pl
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_BUCKET',
    'OUTPUT_BUCKET',
    'DATABASE_NAME',
    'TABLE_NAME'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # 1. S3 から CSV 読み込み (Spark)
    input_path = f"s3://{args['INPUT_BUCKET']}/raw/"
    df_spark = spark.read.csv(input_path, header=True, inferSchema=True)

    # 2. Spark → Pandas → Polars 変換（スキーマ進化ロジック）
    df_pandas = df_spark.toPandas()
    df_polars = pl.from_pandas(df_pandas)

    # 3. Polars でスキーマ進化（Phase 0 と同じ）
    new_columns = ['id', 'name', 'email', 'created_at', 'status', 'verified_at']
    for col in new_columns:
        if col not in df_polars.columns:
            df_polars = df_polars.with_columns(
                pl.lit(None).cast(pl.String).alias(col)
            )
    df_polars = df_polars.select(new_columns)

    # 4. Polars → Pandas → Spark 変換
    df_pandas_out = df_polars.to_pandas()
    df_spark_out = spark.createDataFrame(df_pandas_out)

    # 5. Parquet を S3 に書き込み
    output_path = f"s3://{args['OUTPUT_BUCKET']}/processed/"
    df_spark_out.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(output_path)

    # 6. Glue Catalog にテーブル登録
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(df_spark_out, glueContext, "output_frame"),
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": []
        },
        format="parquet",
        transformation_ctx="output"
    )

    # 7. Glue Catalog 直接操作（メタデータ登録）
    client = boto3.client('glue')
    client.update_table(
        DatabaseName=args['DATABASE_NAME'],
        TableInput={
            'Name': args['TABLE_NAME'],
            'StorageDescriptor': {
                'Columns': [{'Name': col, 'Type': 'string'} for col in new_columns],
                'Location': output_path,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                }
            }
        }
    )

    job.commit()
    print("ETL completed successfully")

except Exception as e:
    print(f"Error: {str(e)}")
    job.commit()
    raise
```

---

### 2.3 AWS Glue Catalog

**目的：** 統一メタデータレジストリ（SQLite の代替）

```yaml
Database Name: streaming_etl

Tables:
  - streaming_etl_schema
    Partition Keys: []
    Columns:
      - id: bigint
      - name: string
      - email: string
      - created_at: string
      - status: string
      - verified_at: string
    Location: s3://streaming-etl-xxx/processed/
    Format: Parquet

Metadata Properties:
  classification: parquet
  version: 1
  schema_version: 1
  created_at: 2026-05-24
```

**利点（DynamoDB vs Glue Catalog）：**

| 比較項目         | DynamoDB | Glue Catalog |
| ---------------- | -------- | ------------ |
| **セットアップ** | 簡単     | やや複雑     |
| **Iceberg 統合** | ❌       | ✅           |
| **Trino 連携**   | ❌       | ✅           |
| **スキーマ推論** | 手動     | 自動         |
| **本番対応**     | △        | ✅✅✅       |

---

### 2.4 EventBridge Rule + CloudWatch Events

**目的：** S3 アップロード時に Glue Job を自動トリガー

```yaml
Rule Name: csv-upload-trigger

Event Source: AWS Events

Event Pattern:
  source: ["aws.s3"]
  detail-type: ["Object Created"]
  detail:
    bucket:
      name: ["streaming-etl-xxx"]
    object:
      key:
        - prefix: "raw/"

Target: Glue Job

Target Configuration:
  RoleArn: arn:aws:iam::123456789:role/EventBridgeGlueRole

Job Parameters:
  --INPUT_BUCKET: streaming-etl-xxx
  --OUTPUT_BUCKET: streaming-etl-xxx
  --DATABASE_NAME: streaming_etl
  --TABLE_NAME: streaming_etl_schema

Retry Policy:
  MaximumEventAge: 3600
  MaximumRetryAttempts: 2
```

**CloudWatch イベント設定（Glue Job 監視）：**

```yaml
EventBridgeRule: glue-job-completion

Event Pattern:
  source: ["aws.glue"]
  detail-type: ["Glue Job State Change"]
  detail:
    jobName: ["streaming-etl-phase0-transformer"]
    state: ["SUCCEEDED", "FAILED"]

Target:
  - SNS Topic（通知）
  - CloudWatch Logs（ログ記録）
```

---

### 2.5 CloudWatch Logs & Monitoring

**目的：** Glue Job 実行ログ・メトリクス監視

```yaml
Log Group: /aws/glue/jobs/streaming-etl-phase0-transformer
Retention: 30 days

Metrics:
  - glue.driver.aggregate.numFailedTasks
  - glue.driver.aggregate.numSuccessfulTasks
  - glue.driver.system.cpuSystemLoad
  - glue.driver.system.jvm.mem.heapUsed

Alarms:
  - Job Duration > 30 min
  - Job Failed (FAILED state)
  - Error Count > 10

Dashboard:
  - Job Execution Timeline
  - Success Rate
  - DPU Usage
```

---

## 3. Glue vs Lambda の設計判断

### なぜ Glue を選んだか

| 判定項目               | Lambda   | Glue Jobs     | 選択     |
| ---------------------- | -------- | ------------- | -------- |
| **スケーラビリティ**   | 制限あり | ✅ 無制限     | Glue     |
| **Iceberg 対応**       | 手動     | ✅ ネイティブ | Glue     |
| **Glue Catalog 連携**  | 手動     | ✅ 自動       | Glue     |
| **PySpark サポート**   | △        | ✅ フル       | Glue     |
| **本番運用対応**       | △        | ✅✅✅        | Glue     |
| **セットアップ複雑度** | 簡単     | ⚠️ やや複雑   | Lambda   |
| **本件のゴール適合度** | 40%      | ✅ 95%        | **Glue** |

**結論：** Phase 2（Iceberg）への拡張性・本番運用性を重視 → **Glue 採用**

---

## 4. Phase 0 との差分実装

### 4.1 ローカル（Phase 0）

```python
# phase0_manual_transform.py
import polars as pl
import sqlite3

df = pl.read_csv('specs/sample_columns.csv')
# スキーマ進化
new_columns = ['id', 'name', 'email', 'created_at', 'status', 'verified_at']
for col in new_columns:
    if col not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.String).alias(col))
df = df.select(new_columns)

# 出力
df.write_parquet('data/sample.parquet')

# SQLite メタデータ
conn = sqlite3.connect('data/schema_metadata.sqlite')
```

### 4.2 Glue（Phase 0.5）- 差分のみ

```python
# glue_job.py
# スキーマ進化ロジックは完全同じ（Polars 部分）
# 差分は I/O と Glue Catalog 連携のみ

# I/O: ファイルシステム → S3
input_path = f"s3://{INPUT_BUCKET}/raw/"  # S3 から読み込み
output_path = f"s3://{OUTPUT_BUCKET}/processed/"  # S3 に書き込み

# メタデータ: SQLite → Glue Catalog
client.update_table(...)  # Glue Catalog にテーブル登録
```

**コア Polars ロジックは 100% 再利用可能！**

---

## 5. デプロイ方式（CloudFormation）

```yaml
# template.yaml
AWSTemplateFormatVersion: "2010-09-09"
Description: Streaming ETL Phase 0.5 (Glue-based)

Parameters:
  EnvironmentName:
    Type: String
    Default: dev
    AllowedValues: [dev, staging, prod]

  GlueJobWorkers:
    Type: Number
    Default: 2

Resources:
  # S3 Bucket
  ETLDataLakeBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "streaming-etl-${AWS::AccountId}-${AWS::Region}"
      VersioningConfiguration:
        Status: Enabled
      LifecycleConfiguration:
        Rules:
          - Id: TransitionRawToGlacier
            Prefix: raw/
            Status: Enabled
            Transitions:
              - TransitionInDays: 30
                StorageClass: GLACIER
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true

  # Glue Database
  GlueDatabase:
    Type: AWS::Glue::Database
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseInput:
        Name: streaming_etl
        Description: Streaming ETL Metadata Catalog

  # IAM Role for Glue Job
  GlueJobRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: glue.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
      Policies:
        - PolicyName: S3Access
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Effect: Allow
                Action:
                  - s3:GetObject
                  - s3:PutObject
                Resource:
                  - !Sub "arn:aws:s3:::${ETLDataLakeBucket}/*"
        - PolicyName: GlueCatalogAccess
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Effect: Allow
                Action:
                  - glue:GetDatabase
                  - glue:GetTable
                  - glue:UpdateTable
                  - glue:PutDataCatalogEncryptionSettings
                Resource: "*"

  # Glue Job
  ETLTransformerJob:
    Type: AWS::Glue::Job
    Properties:
      Name: streaming-etl-phase0-transformer
      Role: !GetAtt GlueJobRole.Arn
      Command:
        Name: glueetl
        ScriptLocation: !Sub "s3://${ETLDataLakeBucket}/scripts/glue_job.py"
        PythonVersion: "3.11"
      DefaultArguments:
        "--INPUT_BUCKET": !Ref ETLDataLakeBucket
        "--OUTPUT_BUCKET": !Ref ETLDataLakeBucket
        "--DATABASE_NAME": !Ref GlueDatabase
        "--TABLE_NAME": "streaming_etl_schema"
        "--job-bookmark-option": "job-bookmark-enable"
        "--TempDir": !Sub "s3://${ETLDataLakeBucket}/temp/"
      ExecutionProperty:
        MaxConcurrentRuns: 2
      GlueVersion: "4.0"
      WorkerType: G.2X
      NumberOfWorkers: !Ref GlueJobWorkers
      Timeout: 30

  # EventBridge Rule
  S3UploadEventRule:
    Type: AWS::Events::Rule
    Properties:
      Name: csv-upload-trigger
      EventPattern:
        source:
          - aws.s3
        detail-type:
          - Object Created
        detail:
          bucket:
            name:
              - !Ref ETLDataLakeBucket
          object:
            key:
              - prefix: raw/
      State: ENABLED
      Targets:
        - Arn: !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:job/${ETLTransformerJob}"
          RoleArn: !GetAtt EventBridgeGlueRole.Arn
          GlueParameters:
            JobName: !Ref ETLTransformerJob

  # IAM Role for EventBridge
  EventBridgeGlueRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: events.amazonaws.com
            Action: sts:AssumeRole
      Policies:
        - PolicyName: GlueJobAccess
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Effect: Allow
                Action:
                  - glue:NotifyEvent
                  - glue:StartJobRun
                Resource: !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:job/${ETLTransformerJob}"

Outputs:
  DataLakeBucket:
    Value: !Ref ETLDataLakeBucket
  GlueJobName:
    Value: !Ref ETLTransformerJob
  GlueCatalogDatabase:
    Value: !Ref GlueDatabase
```

---

## 6. コスト見積もり（月額）

| リソース            | 無料枠         | 小規模     | 中規模      |
| ------------------- | -------------- | ---------- | ----------- |
| **S3**              | 5GB            | $0.10-0.50 | $1-5        |
| **Glue Job (DPU)**  | 1M objects     | $5-10      | $50-100     |
| **Glue Catalog**    | 100 objects    | $0.50      | $1-2        |
| **EventBridge**     | 100,000 events | $0         | $0-1        |
| **CloudWatch Logs** | 5GB            | $0         | $1-2        |
| **合計**            |                | **$5-10**  | **$50-110** |

**本番導入時は Reserved Capacity で 30-40% 削減可能**

---

## 7. 開発ステップ

### Step 1: AWS 環境セットアップ（30分）

```bash
# AWS CLI 設定確認
aws configure list

# CloudFormation テンプレート検証
aws cloudformation validate-template --template-body file://template.yaml

# S3 bucket 作成（スクリプト保存用）
aws s3 mb s3://streaming-etl-scripts-{account-id}
```

### Step 2: Glue スクリプト作成（2時間）

```
src/glue/
  ├─ glue_job.py             ← Polars ロジック + Glue 統合
  ├─ requirements.txt         ← polars, pyspark
  ├─ __init__.py
  └─ tests/
     └─ test_glue_job.py
```

### Step 3: CloudFormation テンプレート（1.5時間）

```
aws/
  └─ template.yaml            ← S3, Glue Job, Catalog, EventBridge
```

### Step 4: ローカルテスト（1.5時間）

```bash
# Glue Job スクリプトをローカルで PySpark 実行
python src/glue/glue_job.py
```

### Step 5: AWS デプロイ（30分）

```bash
# CloudFormation スタック作成
aws cloudformation create-stack \
  --stack-name streaming-etl-phase0-5 \
  --template-body file://aws/template.yaml

# Glue スクリプトを S3 にアップロード
aws s3 cp src/glue/glue_job.py s3://streaming-etl-xxx/scripts/
```

### Step 6: 統合テスト（1.5時間）

```bash
# テスト CSV を S3 にアップロード
aws s3 cp specs/sample_columns.csv s3://streaming-etl-xxx/raw/

# Glue Job が自動トリガーされるのを確認
aws glue get-job-runs --job-name streaming-etl-phase0-transformer

# 出力ファイルが生成されたか確認
aws s3 ls s3://streaming-etl-xxx/processed/

# Glue Catalog にテーブルが登録されたか確認
aws glue get-table --database-name streaming_etl --name streaming_etl_schema
```

**合計：約 7-8 時間（習熟度による）**

---

## 8. テスト戦略

### 8.1 ユニットテスト（ローカル、PySpark）

```python
# tests/test_glue_job.py
import pytest
from pyspark.sql import SparkSession
from glue_job import evolve_schema_polars, validate_schema

@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("test") \
        .master("local") \
        .getOrCreate()

def test_polars_schema_evolution():
    """Polars スキーマ進化をテスト"""
    import polars as pl
    df = pl.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob']
    })
    result = evolve_schema_polars(df)
    assert 'verified_at' in result.columns
    assert result['verified_at'].null_count() == len(result)

def test_glue_catalog_integration(spark):
    """Glue Catalog 統合テスト（AWS 上で実行）"""
    # AWS 環境でのみ実行
    pass
```

### 8.2 統合テスト（AWS）

```python
# tests/test_aws_integration.py
import boto3
import pytest
import time

@pytest.fixture
def s3_client():
    return boto3.client('s3')

@pytest.fixture
def glue_client():
    return boto3.client('glue')

def test_end_to_end_etl(s3_client, glue_client):
    """E2E: CSV Upload → Glue Job Trigger → Parquet Output"""

    # 1. テスト CSV を S3 にアップロード
    s3_client.put_object(
        Bucket='streaming-etl-xxx',
        Key='raw/test_input.csv',
        Body=b'id,name,email,created_at,status\n1,Alice,alice@example.com,2025-01-01,active\n'
    )

    # 2. Glue Job トリガーを待つ
    time.sleep(10)

    # 3. Job 実行状態を確認
    jobs = glue_client.get_job_runs(
        JobName='streaming-etl-phase0-transformer',
        MaxResults=1
    )
    assert len(jobs['JobRuns']) > 0
    assert jobs['JobRuns'][0]['JobRunState'] in ['RUNNING', 'SUCCEEDED']

    # 4. 出力ファイルを確認
    time.sleep(30)  # 処理待機
    response = s3_client.list_objects_v2(
        Bucket='streaming-etl-xxx',
        Prefix='processed/'
    )
    assert 'Contents' in response
    assert len(response['Contents']) > 0

    # 5. Glue Catalog テーブルを確認
    table = glue_client.get_table(
        DatabaseName='streaming_etl',
        Name='streaming_etl_schema'
    )
    assert table['Table']['StorageDescriptor']['Columns']
    assert len(table['Table']['StorageDescriptor']['Columns']) == 6  # verified_at を含む
```

---

## 8. Monitor & Logging

### 8.1 CloudWatch ダッシュボード

```yaml
Metrics:
  - glue.driver.aggregate.numFailedTasks
  - glue.driver.aggregate.numSuccessfulTasks
  - glue.driver.system.cpuSystemLoad
  - glue.driver.system.jvm.mem.heapUsed

Alarms:
  - Job Duration > 30 min
  - Job Failed (FAILED state)
  - Error Count > 10

Dashboard:
  - Job Execution Timeline
  - Success Rate
  - DPU Usage
```

### 8.2 ログクエリ例

```
# エラーを検索
fields @timestamp, @message
| filter @message like /ERROR/
| stats count() as error_count by @timestamp

# 実行時間を分析
fields @duration
| stats avg(@duration) as avg_duration, max(@duration) as max_duration
```

---

## 9. Iceberg 拡張への道筋（Phase 2）

```
Phase 0.5-Glue（この段階）
  ├─ Glue Catalog にメタデータ登録 ✅
  └─ Parquet フォーマット出力 ✅

        ↓ スムーズに拡張可能

Phase 2-Iceberg
  ├─ Parquet → Iceberg テーブル変換（1行コード変更）
  ├─ Glue Catalog が自動で Iceberg メタデータ管理
  └─ Trino で ACID クエリ実行可能
```

**主な変更点：**

```python
# Phase 0.5（Parquet）
df_spark_out.write.mode("overwrite").format("parquet").save(output_path)

# Phase 2（Iceberg）- ほぼ同じ
df_spark_out.write.mode("overwrite").format("iceberg").save(output_path)
```

---

## 10. 次ステップ

1. ✅ この設計書をレビュー
2. AWS 環境確認（CLI 実行可能か）
3. `AWS_DEPLOYMENT_GUIDE.md` 作成（実装手順書）
4. CloudFormation テンプレート作成
5. Glue スクリプト実装開始

---

**質問・修正ありますか？**
