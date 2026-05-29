output "glue_table_name" {
  description = "Glue Catalog table name"
  value       = aws_glue_catalog_table.video_advertisement.name
}

output "glue_table_arn" {
  description = "Glue Catalog table ARN"
  value       = aws_glue_catalog_table.video_advertisement.arn
}

output "athena_workgroup_name" {
  description = "Athena Workgroup name"
  value       = aws_athena_workgroup.video_advertisement.name
}

output "athena_workgroup_arn" {
  description = "Athena Workgroup ARN"
  value       = aws_athena_workgroup.video_advertisement.arn
}

output "select_all_query_id" {
  description = "Athena Named Query ID for SELECT *"
  value       = aws_athena_named_query.select_all.id
}

output "count_records_query_id" {
  description = "Athena Named Query ID for COUNT by date"
  value       = aws_athena_named_query.count_records.id
}

output "top_videos_query_id" {
  description = "Athena Named Query ID for TOP 10 videos"
  value       = aws_athena_named_query.top_videos.id
}
