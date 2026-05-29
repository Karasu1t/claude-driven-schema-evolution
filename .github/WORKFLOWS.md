# CI/CD Workflows - Iceberg ETL Pipeline

自動デプロイ、テスト、インフラ破棄のための GitHub Actions ワークフロー

## 📋 Available Workflows

### 1. `terraform_apply.yml` - Auto Deploy Infrastructure

**Trigger:**

- `push` to `main` branch (Terraform or Glue code changes)
- Manual: `workflow_dispatch`

**What It Does:**

1. ✅ Checkout code
2. ✅ Configure AWS credentials (from GitHub Secrets)
3. ✅ Terraform init → plan → apply
4. ✅ Deploy: Glue Job, Lambda, EventBridge, S3, Athena, IAM
5. ✅ Output deployment summary

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

### 2. `terraform_destroy.yml` - Safe Infrastructure Teardown

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

### 3. `integration_test.yml` - End-to-End Testing

**Trigger:**

- Manual: `workflow_dispatch`

**What It Does:**

1. ✅ Create test CSV (default: 20260529)
2. ✅ Upload to S3 raw bucket
3. ✅ Invoke Lambda function
4. ✅ Wait 90 seconds for Glue Job
5. ✅ Query Athena to verify data
6. ✅ Report test results

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
GitHub Actions → integration_test.yml → "Run workflow"

# Custom date
GitHub Actions → integration_test.yml → "Run workflow"
Input: test_date = "YYYYMMDD" (e.g., "20260530")
```

---

## 🔐 Required GitHub Secrets

Set these in: Settings → Secrets and variables → Actions

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION (optional, defaults to ap-northeast-1)
```

---

## 📊 Typical Workflow Sequence

### Option A: Deploy + Test

```
1. terraform_apply.yml (auto or manual)
   ↓
2. integration_test.yml (manual)
   ↓
3. Verify results in AWS Console
```

### Option B: Code Change → Auto Deploy

```
1. Modify: terraform/modules/*.tf or src/glue/glue_job.py
2. git push origin main
   ↓
3. terraform_apply.yml (auto)
   ↓
4. Manual test via integration_test.yml
```

### Option C: Clean Up

```
1. terraform_destroy.yml (manual, requires confirmation)
   ↓
2. All resources deleted
```

---

## ⚠️ Important Notes

1. **AWS Credentials**: Must be set as GitHub Secrets
2. **Region**: Hardcoded to `ap-northeast-1` (modify if needed)
3. **Cost**: Terraform will create actual AWS resources (incurs charges)
4. **Test Isolation**: Integration tests use actual AWS resources (not mocked)
5. **Destroy Safety**: Requires explicit "confirm" input to prevent accidents

---

## 🚀 First-Time Setup

### Step 1: Add GitHub Secrets

```
Settings → Secrets and variables → Actions → New repository secret

Name: AWS_ACCESS_KEY_ID
Value: <your-aws-access-key>

Name: AWS_SECRET_ACCESS_KEY
Value: <your-aws-secret-key>
```

### Step 2: Deploy Infrastructure

```
GitHub Actions → terraform_apply → "Run workflow" → Wait 2-3 min
```

### Step 3: Run Integration Test

```
GitHub Actions → integration_test → "Run workflow" (test_date=20260529)
```

### Step 4: Verify in AWS Console

```
- Glue: dev-karasuit-schema-evolution-etl job
- S3: iceberg-warehouse/ directory
- Athena: SELECT COUNT(*) FROM video_advertisement
```

---

## 📝 Customization

**Change Glue Job Parameters:**
Edit: `terraform/modules/aws/glue_job/glue_job.tf`
→ terraform_apply will auto-update

**Change Test Data:**
Edit: `integration_test.yml` → Create test CSV section

**Change Deployment Region:**
Edit: `env: AWS_REGION: ap-northeast-1` in each workflow

**Add More Triggers:**
Edit: `on:` section in each workflow (add webhooks, schedules, etc.)

---

## 🐛 Troubleshooting

| Issue                 | Solution                                          |
| --------------------- | ------------------------------------------------- |
| AWS credentials error | Verify GitHub Secrets are set correctly           |
| Terraform init fails  | Check AWS region and IAM permissions              |
| Glue Job timeout      | Increase timeout in glue_job.tf (default: 30 min) |
| Athena query fails    | Wait 2 min after Glue Job completes               |
| Cost spike            | Run terraform_destroy to clean up resources       |

---

## Next Steps

- [ ] Set up GitHub Secrets (AWS credentials)
- [ ] Test terraform_apply workflow
- [ ] Run integration_test with sample data
- [ ] Monitor CloudWatch logs for Glue Job
- [ ] Verify Athena queries return data
- [ ] Document any customizations
