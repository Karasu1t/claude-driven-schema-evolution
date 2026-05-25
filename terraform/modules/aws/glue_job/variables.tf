variable "job_name" {
  description = "Name of the Glue Job"
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

variable "input_bucket" {
  description = "S3 bucket for input CSV files"
  type        = string
}

variable "output_bucket" {
  description = "S3 bucket for output Parquet/Iceberg files"
  type        = string
}

variable "script_location" {
  description = "S3 location of the Glue Job script"
  type        = string
}

variable "glue_script_source_path" {
  description = "Local path to Glue Job script"
  type        = string
  default     = null
}

variable "glue_version" {
  description = "Glue version"
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Glue worker type"
  type        = string
  default     = "G.2X"
}

variable "num_workers" {
  description = "Number of Glue workers"
  type        = number
  default     = 2
}

variable "timeout_minutes" {
  description = "Job timeout in minutes"
  type        = number
  default     = 30
}

variable "max_retries" {
  description = "Maximum retry attempts"
  type        = number
  default     = 1
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}
