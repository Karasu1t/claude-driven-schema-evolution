output "athena_workgroup_name" {
  value       = aws_athena_workgroup.iceberg_workgroup.name
  description = "Name of the Athena workgroup for Iceberg queries"
}

output "athena_workgroup_arn" {
  value       = aws_athena_workgroup.iceberg_workgroup.arn
  description = "ARN of the Athena workgroup"
}

output "athena_results_bucket" {
  value       = aws_s3_bucket.athena_results_bucket.bucket
  description = "S3 bucket for Athena query results"
}

output "glue_database_name" {
  value       = aws_glue_catalog_database.iceberg_database.name
  description = "Glue Catalog database name for Iceberg tables"
}

output "sample_query_id" {
  value       = aws_athena_named_query.iceberg_sample_query.id
  description = "ID of the sample Athena query"
}
