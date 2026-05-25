output "eventbridge_rule_name" {
  description = "EventBridge scheduled rule name"
  value       = aws_cloudwatch_event_rule.scheduled_glue_rule.name
}

output "eventbridge_rule_arn" {
  description = "EventBridge scheduled rule ARN"
  value       = aws_cloudwatch_event_rule.scheduled_glue_rule.arn
}

output "eventbridge_rule_schedule" {
  description = "EventBridge rule schedule expression (Cron)"
  value       = aws_cloudwatch_event_rule.scheduled_glue_rule.schedule_expression
}

output "eventbridge_role_arn" {
  description = "EventBridge IAM role ARN"
  value       = aws_iam_role.eventbridge_role.arn
}
