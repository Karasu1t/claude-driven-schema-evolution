# Athena Workgroup for querying Iceberg tables
resource "aws_athena_workgroup" "iceberg_workgroup" {
  name = "${var.environment}-${var.project}-iceberg-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.athena_results_bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "AUTO"
    }
  }

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-${var.project}-iceberg-workgroup"
      Environment = var.environment
      Project     = var.project
    }
  )

  depends_on = [
    aws_s3_bucket.athena_results_bucket,
    aws_s3_bucket_public_access_block.athena_results_public_access_block
  ]
}

# S3 bucket for Athena query results
resource "aws_s3_bucket" "athena_results_bucket" {
  bucket = "${var.environment}-${var.project}-athena-results-bucket"

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-${var.project}-athena-results"
      Environment = var.environment
      Project     = var.project
    }
  )
}

# Block public access to Athena results bucket
resource "aws_s3_bucket_public_access_block" "athena_results_public_access_block" {
  bucket = aws_s3_bucket.athena_results_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Athena Glue Catalog database for Iceberg tables
resource "aws_glue_catalog_database" "iceberg_database" {
  name = "${var.environment}_${var.project}_iceberg_db"

  description = "Glue Catalog database for Iceberg tables (video advertisement data)"
}

# Athena named query (sample query for Iceberg table)
resource "aws_athena_named_query" "iceberg_sample_query" {
  name        = "${var.environment}-${var.project}-iceberg-sample-query"
  description = "Sample query to verify Iceberg table integration with Athena"
  database    = aws_glue_catalog_database.iceberg_database.name
  query       = "SELECT * FROM video_advertisement_data LIMIT 10;"
  workgroup   = aws_athena_workgroup.iceberg_workgroup.name
}
