output "eventbridge_rule_name" {
  description = "EventBridge rule name"
  value       = aws_cloudwatch_event_rule.s3_put_rule.name
}

output "eventbridge_rule_arn" {
  description = "EventBridge rule ARN"
  value       = aws_cloudwatch_event_rule.s3_put_rule.arn
}

output "eventbridge_role_arn" {
  description = "EventBridge IAM role ARN"
  value       = aws_iam_role.eventbridge_role.arn
}
