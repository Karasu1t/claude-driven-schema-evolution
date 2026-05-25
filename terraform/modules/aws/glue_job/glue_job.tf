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
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-bookmark-option"   = "job-bookmark-enable"
    "--INPUT_BUCKET"          = var.input_bucket
    "--OUTPUT_BUCKET"         = var.output_bucket
    "--enable-spark-ui"       = "true"
    "--spark-event-logs-path" = "s3://${var.output_bucket}/spark-logs/"
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
    aws_iam_role_policy.glue_job_policy
  ]
}
