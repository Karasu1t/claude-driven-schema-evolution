# EventBridge rule for S3 → Glue Job trigger
resource "aws_cloudwatch_event_rule" "s3_put_rule" {
  name        = "${var.environment}-${var.project}-s3-put-rule"
  description = "Trigger Glue Job on S3 raw bucket PUT"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.raw_bucket_name]
      }
      object = {
        key = [{
          prefix = "raw/"
        }]
      }
    }
  })

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-${var.project}-s3-put-rule"
      Environment = var.environment
      Project     = var.project
    }
  )
}

# EventBridge target: Glue Job
resource "aws_cloudwatch_event_target" "glue_job_target" {
  rule      = aws_cloudwatch_event_rule.s3_put_rule.name
  target_id = "GlueJobTarget"
  arn       = "arn:aws:glue:${data.aws_caller_identity.current.account}:job/${var.glue_job_name}"
  role_arn  = aws_iam_role.eventbridge_role.arn

  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }
    input_template = jsonencode({
      "--INPUT_BUCKET"  = "<bucket>"
      "--OUTPUT_BUCKET" = "<bucket>"
    })
  }
}

# IAM role for EventBridge to invoke Glue Job
resource "aws_iam_role" "eventbridge_role" {
  name = "${var.environment}-${var.project}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-${var.project}-eventbridge-role"
      Environment = var.environment
      Project     = var.project
    }
  )
}

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "${var.environment}-${var.project}-eventbridge-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:NotifyEvent",
          "glue:StartJobRun"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_caller_identity.current.account}:job/${var.glue_job_name}"
        ]
      }
    ]
  })
}

# Data source to get current AWS account ID
data "aws_caller_identity" "current" {}
