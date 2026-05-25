output "glue_job_name" {
  description = "Name of the Glue Job"
  value       = aws_glue_job.etl_job.name
}

output "glue_job_arn" {
  description = "ARN of the Glue Job"
  value       = aws_glue_job.etl_job.arn
}

output "glue_role_arn" {
  description = "ARN of the Glue Job IAM role"
  value       = aws_iam_role.glue_job_role.arn
}

output "glue_role_name" {
  description = "Name of the Glue Job IAM role"
  value       = aws_iam_role.glue_job_role.name
}
