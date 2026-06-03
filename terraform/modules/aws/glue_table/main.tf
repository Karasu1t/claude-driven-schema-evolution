/**
 * Glue Catalog Table Registration
 * Registers Parquet data as external table in Glue Catalog for Athena queries
 */

resource "aws_glue_catalog_table" "video_advertisement" {
  name          = var.table_name
  database_name = var.glue_database
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${var.data_bucket}/processed_data/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Define columns matching Glue Job output
    columns {
      name = "video_title"
      type = "string"
    }

    columns {
      name = "views"
      type = "bigint"
    }

    columns {
      name = "channel_name"
      type = "string"
    }

    columns {
      name = "channel_subscribers"
      type = "bigint"
    }

    columns {
      name = "likes"
      type = "bigint"
    }

    columns {
      name = "video_duration_minutes"
      type = "bigint"
    }

    columns {
      name = "processed_at"
      type = "string"
    }

    columns {
      name = "glue_job_run_id"
      type = "string"
    }

    columns {
      name = "ver_date"
      type = "string"
    }
  }

  parameters = {
    "EXTERNAL"            = "TRUE"
    "transactionality"    = "false"
    "parquet.compression" = "snappy"
    "classification"      = "parquet"
  }
}

# Allow Athena to read from the data location
resource "aws_s3_bucket_policy" "athena_read_policy" {
  bucket = var.data_bucket

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaReadParquetData"
        Effect = "Allow"
        Principal = {
          Service = "athena.amazonaws.com"
        }
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.data_bucket}",
          "arn:aws:s3:::${var.data_bucket}/*"
        ]
      }
    ]
  })
}

# Athena Workgroup for querying the data
resource "aws_athena_workgroup" "video_advertisement" {
  name = "${var.environment}-${var.project}-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    result_configuration {
      output_location = "s3://${var.results_bucket}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Athena Named Query for quick data exploration
resource "aws_athena_named_query" "select_all" {
  name        = "${var.environment}-${var.project}-select-all"
  description = "Select all video advertisement records"
  database    = var.glue_database
  query       = "SELECT * FROM ${var.table_name} LIMIT 100;"
  workgroup   = aws_athena_workgroup.video_advertisement.name
}

resource "aws_athena_named_query" "count_records" {
  name        = "${var.environment}-${var.project}-count-records"
  description = "Count total records by date"
  database    = var.glue_database
  query       = "SELECT ver_date, COUNT(*) as record_count FROM ${var.table_name} GROUP BY ver_date ORDER BY ver_date DESC;"
  workgroup   = aws_athena_workgroup.video_advertisement.name
}

resource "aws_athena_named_query" "top_videos" {
  name        = "${var.environment}-${var.project}-top-videos"
  description = "Top 10 videos by views"
  database    = var.glue_database
  query       = "SELECT video_title, views, channel_name FROM ${var.table_name} ORDER BY views DESC LIMIT 10;"
  workgroup   = aws_athena_workgroup.video_advertisement.name
}
