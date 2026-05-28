resource "aws_s3_bucket" "s3_raw_bucket" {
  bucket        = "${var.environment}-${var.project}-raw-bucket"
  force_destroy = true

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-${var.project}-raw-bucket"
      Environment = var.environment
      Project     = var.project
      Purpose     = "Raw Data Input"
    }
  )
}

resource "aws_s3_bucket_ownership_controls" "bucket_ownership_control" {
  bucket = aws_s3_bucket.s3_raw_bucket.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_versioning" "raw_bucket_versioning" {
  bucket = aws_s3_bucket.s3_raw_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

