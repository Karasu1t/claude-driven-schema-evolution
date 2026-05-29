# CI/CD Workflows - Iceberg ETL Pipeline

自動デプロイ、テスト（UT + E2E）、インフラ破棄のための GitHub Actions ワークフロー

## 📋 Workflow Overview

| Workflow                | Type           | Trigger                      | Purpose                          |
| ----------------------- | -------------- | ---------------------------- | -------------------------------- |
| `unit_test.yml`         | UT             | `push` to src/glue or tests/ | Pytest schema logic validation   |
| `terraform_apply.yml`   | Infrastructure | `push` to main               | Auto-deploy AWS resources        |
| `e2e_test.yml`          | E2E            | Manual `workflow_dispatch`   | End-to-end pipeline verification |
| `terraform_destroy.yml` | Infrastructure | Manual (requires confirm)    | Safe infrastructure teardown     |

---

## 🧪 Test Strategy

### Unit Tests (UT)

**Purpose:** Validate Glue Job logic without AWS resources

- UT-2: VER_DATE argument validation (required, error handling)
- UT-3: Schema evolution (missing columns auto-added)
- Coverage: Metadata addition, column ordering

**Speed:** ⚡ Seconds (no AWS resources)

**Cost:** 💰 Free (local Spark session)

**Location:** `tests/test_glue_job.py`

---

### End-to-End Tests (E2E)

**Purpose:** Verify complete pipeline: CSV → Lambda → Glue → Athena

- CSV upload to S3 raw bucket
- Lambda triggers Glue Job
- Glue transforms data and writes Iceberg table
- Athena queries verify data integrity

**Speed:** 🐌 5-10 minutes (includes job execution)

**Cost:** 💵 On-demand AWS resource usage

**Location:** `.github/workflows/e2e_test.yml`

---

## 📋 Available Workflows

### 1. `unit_test.yml` - Unit Tests

**Trigger:**

- `push` to branches (src/glue/** or tests/**)
- Manual: `workflow_dispatch`
- Pull requests with test changes

**What It Does:**

1. ✅ Checkout code
2. ✅ Install pytest + pyspark
3. ✅ Run pytest test suite
4. ✅ Test on Python 3.9 & 3.11

**Test Cases:**

```
✓ VER_DATE validation (required argument)
✓ VER_DATE extraction (parse from sys.argv)
✓ Schema evolution (add missing columns)
✓ Metadata addition (processed_at, glue_job_run_id)
✓ Column order preservation
```

**Usage:**

```bash
# Automatic: Push code changes
git push origin main

# Manual: GitHub Actions → unit_test.yml → "Run workflow"

# Local test:
cd tests && pip install -r requirements.txt
pytest test_glue_job.py -v
```

**Output:**

```
test_glue_job.py::TestVERDateValidation::test_ver_date_extraction_valid PASSED
test_glue_job.py::TestSchemaEvolution::test_schema_evolution_add_missing_columns PASSED
...
```

---

### 2. `terraform_apply.yml` - Auto Deploy Infrastructure

**Trigger:**

- `push` to `main` branch (Terraform or Glue code changes)
- Manual: `workflow_dispatch`

**What It Does:**

1. ✅ Checkout code
2. ✅ Configure AWS credentials (from GitHub Secrets)
3. ✅ Terraform init → plan → apply
4. ✅ Deploy: Glue Job, Lambda, EventBridge, S3, Athena, IAM

**Files Monitored:**

```
- terraform/modules/**
- terraform/env/dev/aws/**
- src/glue/**
```

**Usage:**

```bash
# Automatic: Push changes to main
git push origin main

# Manual: GitHub Actions → terraform_apply.yml → "Run workflow"
```

---

### 3. `e2e_test.yml` - End-to-End Testing

**Trigger:**

- Manual: `workflow_dispatch`

**What It Does:**

1. ✅ Create test CSV (default: 20260529, customizable)
2. ✅ Upload to S3 raw bucket
3. ✅ Invoke Lambda function
4. ✅ Wait 90 seconds for Glue Job execution
5. ✅ Query Athena to verify Iceberg table
6. ✅ Validate data integrity

**Test Data:**

```
3 sample rows:
- test_video_1, test_video_2, test_video_3
- Channels: TestChannel1, TestChannel2, TestChannel3
- Views range: 50K-100K
```

**Usage:**

```bash
# Default date (20260529)
GitHub Actions → e2e_test.yml → "Run workflow"

# Custom date
GitHub Actions → e2e_test.yml → "Run workflow"
Input: test_date = "YYYYMMDD" (e.g., "20260530")
```

---

### 4. `terraform_destroy.yml` - Safe Infrastructure Teardown

**Trigger:**

- Manual only: `workflow_dispatch`

**What It Does:**

1. ✅ Require confirmation (must type 'confirm')
2. ✅ Terraform destroy -auto-approve
3. ✅ Delete all 20 resources

**Safety Feature:**

```
Input: confirm_destroy
Must type: "confirm" (prevents accidental destruction)
```

**Usage:**

```bash
# GitHub Actions → terraform_destroy.yml → "Run workflow"
# Input: confirm_destroy = "confirm"
```

---

## 📊 Typical Workflow Sequences

### Sequence A: Local Dev → Auto Test → Deploy

```
1. Modify: src/glue/glue_job.py
2. git push origin main
   ↓
3. unit_test.yml (auto)
   └─ Pytest: UT-2, UT-3, etc. ← PASS/FAIL feedback in 30 seconds
   ↓
4. terraform_apply.yml (auto)
   └─ Deploy AWS resources ← Ready in 2-3 minutes
   ↓
5. e2e_test.yml (manual)
   └─ Full pipeline verification ← Ready in 5-10 minutes
```

### Sequence B: Test Before Deploy

```
1. Create feature branch
2. Push changes
3. unit_test.yml runs automatically
4. Create PR with test results
5. Review + merge
6. terraform_apply.yml auto-deploys
7. Run e2e_test.yml manually for verification
```

### Sequence C: Emergency Teardown

```
1. GitHub Actions → terraform_destroy.yml
2. Input: confirm_destroy = "confirm"
3. ⚠️ All resources deleted in 2-3 minutes
```

---

## 🔐 Security & Access Control

### Required GitHub Secrets

Set these in: Settings → Secrets and variables → Actions

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION (optional, defaults to ap-northeast-1)
```

### Execution Restrictions

**All workflows are restricted to Karasu1t only.**

- Condition: `if: github.actor == 'Karasu1t'`
- Effect: Only the owner can execute workflows
- Reason: Prevent unauthorized AWS resource creation/deletion

**What happens if someone else tries to run:**

```
Workflow is skipped (not executed)
Message: "Skipped due to condition: github.actor == 'Karasu1t'"
```

This prevents:

- ❌ Unauthorized AWS resource creation
- ❌ Cost overruns from accidental/malicious deployment
- ❌ Infrastructure destruction by unknown users
- ❌ Secrets exposure to pull requests

---

## 📝 First-Time Setup

### Step 1: Add GitHub Secrets

```
Settings → Secrets and variables → Actions → New repository secret

Name: AWS_ACCESS_KEY_ID
Value: <your-aws-access-key>

Name: AWS_SECRET_ACCESS_KEY
Value: <your-aws-secret-key>
```

### Step 2: Run Unit Tests

```
GitHub Actions → unit_test.yml → "Run workflow"
→ Should PASS in 30 seconds
```

### Step 3: Deploy Infrastructure

```
GitHub Actions → terraform_apply → "Run workflow" → Wait 2-3 min
```

### Step 4: Run E2E Test

```
GitHub Actions → e2e_test → "Run workflow" (test_date=20260529)
→ Should PASS in 5-10 min
```

### Step 5: Verify in AWS Console

```
- Glue: dev-karasuit-schema-evolution-etl job
- S3: iceberg-warehouse/ directory
- Athena: SELECT COUNT(*) FROM video_advertisement
```

---

## 🐛 Troubleshooting

| Issue                         | Solution                                                       |
| ----------------------------- | -------------------------------------------------------------- |
| Unit tests fail locally       | `pip install -r tests/requirements.txt && pytest tests/`       |
| AWS credentials error         | Verify GitHub Secrets are set correctly                        |
| Terraform init fails          | Check AWS region and IAM permissions                           |
| Glue Job timeout              | Increase timeout in terraform/modules/aws/glue_job/glue_job.tf |
| Athena query fails            | Wait 2 min after Glue Job completes                            |
| Permission denied on workflow | Only Karasu1t can run workflows (by design)                    |

---

## 📦 Test Requirements

**Unit Test Dependencies** (`tests/requirements.txt`)

```
pytest==7.4.3
pyspark==3.5.0
```

**Install Locally:**

```bash
pip install -r tests/requirements.txt
pytest tests/test_glue_job.py -v
```

---

## 🚀 Next Enhancements

- [ ] Add integration tests (partial AWS resource testing)
- [ ] Add performance benchmarks
- [ ] Add schema validation tests
- [ ] Add Iceberg-specific tests (time-travel, snapshots)
- [ ] Add monitoring/alerting dashboards
- [ ] Add cost estimation per workflow
