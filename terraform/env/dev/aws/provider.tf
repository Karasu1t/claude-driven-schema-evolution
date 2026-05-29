# ------------------------------------
# Terraform Cofiguration
# ------------------------------------
terraform {
  required_version = ">= 1.4"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ---------------------------------------------
# Provider
# Note: In GitHub Actions, AWS credentials come from environment variables
# (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
# set by aws-actions/configure-aws-credentials. Do not use profile.
# In local development, set environment variables or use ~/.aws/credentials
# with export AWS_PROFILE=default
provider "aws" {
  region = local.aws_region
}
