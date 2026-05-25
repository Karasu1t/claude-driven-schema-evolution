# EventBridge rule for scheduled Glue Job execution (daily at 6 AM UTC)
resource "aws_cloudwatch_event_rule" "scheduled_glue_rule" {
  name                = "${var.environment}-${var.project}-scheduled-glue-rule"
  description         = "Trigger Glue Job daily at 6 AM UTC"
  schedule_expression = "cron(0 6 * * ? *)" # Every day at 6 AM UTC

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-${var.project}-scheduled-glue-rule"
      Environment = var.environment
      Project     = var.project
    }
  )
}

# EventBridge target: Glue Job
resource "aws_cloudwatch_event_target" "glue_job_target" {
  rule      = aws_cloudwatch_event_rule.scheduled_glue_rule.name
  target_id = "GlueJobTarget"
  arn       = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${var.glue_job_name}"
  role_arn  = aws_iam_role.eventbridge_role.arn
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
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${var.glue_job_name}"
        ]
      }
    ]
  })
}

# Data sources to get current AWS account ID and region
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
