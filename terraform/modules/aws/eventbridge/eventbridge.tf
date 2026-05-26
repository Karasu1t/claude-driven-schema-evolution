# EventBridge rule for scheduled Lambda invocation (daily at 6 AM UTC)
resource "aws_cloudwatch_event_rule" "scheduled_trigger_rule" {
  name                = "${var.environment}-${var.project}-scheduled-trigger-rule"
  description         = "Trigger Lambda function daily at 6 AM UTC"
  schedule_expression = "cron(0 6 * * ? *)" # Every day at 6 AM UTC

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-${var.project}-scheduled-trigger-rule"
      Environment = var.environment
      Project     = var.project
    }
  )
}

# EventBridge target: Lambda function
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.scheduled_trigger_rule.name
  target_id = "GlueTriggerLambda"
  arn       = var.lambda_function_arn
}

# Permission for EventBridge to invoke Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = split(":", var.lambda_function_arn)[6]
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scheduled_trigger_rule.arn
}

