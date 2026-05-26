output "eventbridge_rule_name" {
  description = "EventBridge scheduled rule name"
  value       = aws_cloudwatch_event_rule.scheduled_trigger_rule.name
}

output "eventbridge_rule_arn" {
  description = "EventBridge scheduled rule ARN"
  value       = aws_cloudwatch_event_rule.scheduled_trigger_rule.arn
}

output "eventbridge_rule_schedule" {
  description = "EventBridge rule schedule expression (Cron)"
  value       = aws_cloudwatch_event_rule.scheduled_trigger_rule.schedule_expression
}

output "lambda_target_arn" {
  description = "Lambda function target ARN"
  value       = var.lambda_function_arn
}

