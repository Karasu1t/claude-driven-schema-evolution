variable "environment" {
  type        = string
  description = "Environment name (e.g., dev, staging, prod)"
}

variable "project" {
  type        = string
  description = "Project name"
}

variable "athena_results_bucket" {
  type        = string
  description = "S3 bucket for Athena query results"
}

variable "tags" {
  type        = map(string)
  description = "Common tags for all resources"
  default     = {}
}
