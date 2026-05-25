variable "environment" {
  description = "Environment name"
  type        = string
}

variable "project" {
  description = "Project name"
  type        = string
}

variable "glue_job_name" {
  description = "Glue Job name"
  type        = string
}

variable "glue_role_arn" {
  description = "Glue Job IAM role ARN"
  type        = string
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}
