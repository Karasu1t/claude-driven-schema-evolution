resource "aws_s3_bucket" "s3_processed_bucket" {
  bucket        = "${var.environment}-${var.project}-processed-bucket"
  force_destroy = true

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-${var.project}-processed-bucket"
      Environment = var.environment
      Project     = var.project
      Purpose     = "Processed Data Output"
    }
  )
}

resource "aws_s3_bucket_ownership_controls" "bucket_ownership_control" {
  bucket = aws_s3_bucket.s3_processed_bucket.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

