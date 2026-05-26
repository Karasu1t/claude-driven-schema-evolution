output "lambda_function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.glue_trigger.arn
}

output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.glue_trigger.function_name
}

output "lambda_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = aws_iam_role.lambda_role.arn
}
