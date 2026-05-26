# Create the zip file for Lambda
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/index.py"
  output_path = "${path.module}/lambda_function.zip"
}

# Lambda function to trigger Glue Job
resource "aws_lambda_function" "glue_trigger" {
  filename      = data.archive_file.lambda_zip.output_path
  function_name = "${var.environment}-${var.project}-${var.function_name}"
  role          = aws_iam_role.lambda_role.arn
  handler       = "index.lambda_handler"
  runtime       = "python3.12"

  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      GLUE_JOB_NAME = var.glue_job_name
    }
  }

  timeout = 60

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-${var.project}-${var.function_name}"
      Environment = var.environment
      Project     = var.project
    }
  )

  depends_on = [
    aws_iam_role_policy.lambda_policy
  ]
}
