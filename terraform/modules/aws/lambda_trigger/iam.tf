# IAM role for Lambda execution
resource "aws_iam_role" "lambda_role" {
  name = "${var.environment}-${var.project}-lambda-glue-trigger-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-${var.project}-lambda-glue-trigger-role"
      Environment = var.environment
      Project     = var.project
    }
  )
}

# Policy to allow Lambda to invoke Glue jobs and write logs
resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.environment}-${var.project}-lambda-glue-policy"
  role = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}
