variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "project" {
  description = "Project name"
  type        = string
}

variable "glue_job_name" {
  description = "Name of the Glue Job to invoke"
  type        = string
}

variable "input_bucket" {
  description = "S3 bucket for input CSV data"
  type        = string
  default     = "dev-karasuit-raw-bucket"
}

variable "output_bucket" {
  description = "S3 bucket for output/Iceberg data"
  type        = string
  default     = "dev-karasuit-processed-bucket"
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}
