##############################################
# S3 Bucket Outputs
##############################################
output "raw_bucket_name" {
  description = "Name of the raw data S3 bucket"
  value       = module.s3_lambda_raw_bucket.bucket_name
}

output "processed_bucket_name" {
  description = "Name of the processed data S3 bucket"
  value       = module.s3_lambda_processed_bucket.bucket_name
}

output "raw_bucket_name_test" {
  description = "Name of the test raw data S3 bucket"
  value       = module.s3_lambda_raw_bucket_test.bucket_name
}

output "processed_bucket_name_test" {
  description = "Name of the test processed data S3 bucket"
  value       = module.s3_lambda_processed_bucket_test.bucket_name
}

##############################################
# Glue Job Outputs
##############################################
output "glue_job_name" {
  description = "Name of the Glue Job"
  value       = module.glue_etl_job.glue_job_name
}

output "glue_job_arn" {
  description = "ARN of the Glue Job"
  value       = module.glue_etl_job.glue_job_arn
}

##############################################
# Lambda Trigger Outputs
##############################################
output "lambda_function_name" {
  description = "Name of the Lambda trigger function"
  value       = module.lambda_trigger.lambda_function_name
}

output "lambda_function_arn" {
  description = "ARN of the Lambda trigger function"
  value       = module.lambda_trigger.lambda_function_arn
}

##############################################
# EventBridge Outputs
##############################################
output "eventbridge_rule_name" {
  description = "Name of the EventBridge rule"
  value       = module.eventbridge_trigger.eventbridge_rule_name
}

output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge rule"
  value       = module.eventbridge_trigger.eventbridge_rule_arn
}

output "eventbridge_schedule" {
  description = "EventBridge schedule expression"
  value       = module.eventbridge_trigger.eventbridge_rule_schedule
}

##############################################
# Athena + Glue Catalog Outputs
##############################################
output "athena_workgroup_name" {
  description = "Name of the Athena workgroup for Iceberg queries"
  value       = module.athena_iceberg.athena_workgroup_name
}

output "athena_workgroup_arn" {
  description = "ARN of the Athena workgroup"
  value       = module.athena_iceberg.athena_workgroup_arn
}

output "athena_results_bucket" {
  description = "S3 bucket for Athena query results"
  value       = module.athena_iceberg.athena_results_bucket
}

output "glue_database_name" {
  description = "Glue Catalog database name for Iceberg tables"
  value       = module.athena_iceberg.glue_database_name
}

##############################################
# Phase 4: Glue Table + Athena Integration
##############################################
# Outputs disabled - Glue table management moved from Terraform to Python script
# Iceberg table created via SQL in glue_job.py instead of Terraform
/*
output "glue_table_name" {
  description = "Glue Catalog table name for Parquet data lake"
  value       = module.glue_table_registration.glue_table_name
}

output "glue_table_arn" {
  description = "ARN of Glue Catalog table"
  value       = module.glue_table_registration.glue_table_arn
}

output "athena_table_workgroup_name" {
  description = "Athena Workgroup for querying Parquet table"
  value       = module.glue_table_registration.athena_workgroup_name
}

output "athena_table_workgroup_arn" {
  description = "ARN of Athena Workgroup for table queries"
  value       = module.glue_table_registration.athena_workgroup_arn
}

output "athena_select_all_query_id" {
  description = "Athena Named Query ID for SELECT * operation"
  value       = module.glue_table_registration.select_all_query_id
}

output "athena_count_records_query_id" {
  description = "Athena Named Query ID for COUNT by date"
  value       = module.glue_table_registration.count_records_query_id
}

output "athena_top_videos_query_id" {
  description = "Athena Named Query ID for TOP 10 videos"
  value       = module.glue_table_registration.top_videos_query_id
}
*/
