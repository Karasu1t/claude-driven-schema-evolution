resource "aws_glue_job" "etl_job" {
  name         = "${var.environment}-${var.project}-${var.job_name}"
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = var.glue_version
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = var.script_location
  }

  execution_property {
    max_concurrent_runs = 2
  }

  default_arguments = {
    "--job-bookmark-option"   = var.environment == "dev" ? "job-bookmark-disable" : "job-bookmark-enable"
    "--INPUT_BUCKET"          = var.input_bucket
    "--OUTPUT_BUCKET"         = var.output_bucket
    "--datalake-formats"      = "iceberg"
    "--enable-spark-ui"       = "true"
    "--spark-event-logs-path" = "s3://${var.output_bucket}/spark-logs/"
    "--extra-py-files"        = "s3://${var.output_bucket}/scripts/schema.py"
  }

  max_retries = var.max_retries
  timeout     = var.timeout_minutes

  worker_type       = var.worker_type
  number_of_workers = var.num_workers

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-${var.project}-${var.job_name}"
      Environment = var.environment
      Project     = var.project
    }
  )

  depends_on = [
    aws_iam_role_policy.glue_job_policy,
    aws_s3_object.glue_script
  ]
}

# Upload Glue script to S3
resource "aws_s3_object" "glue_script" {
  count  = var.glue_script_source_path != null ? 1 : 0
  bucket = var.output_bucket
  key    = "scripts/glue_job.py"
  source = var.glue_script_source_path
  etag   = filemd5(var.glue_script_source_path)

  tags = merge(var.tags, { Name = "glue_job_script" })
}

# Upload schema.py as an extra Python file (imported by glue_job.py)
resource "aws_s3_object" "glue_schema" {
  count  = var.glue_schema_source_path != null ? 1 : 0
  bucket = var.output_bucket
  key    = "scripts/schema.py"
  source = var.glue_schema_source_path
  etag   = filemd5(var.glue_schema_source_path)

  tags = merge(var.tags, { Name = "glue_schema" })
}
