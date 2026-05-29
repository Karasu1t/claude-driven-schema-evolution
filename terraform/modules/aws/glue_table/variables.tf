variable "environment" {
  description = "Environment name (e.g., dev, prod)"
  type        = string
}

variable "project" {
  description = "Project name"
  type        = string
}

variable "glue_database" {
  description = "Glue Catalog database name"
  type        = string
}

variable "table_name" {
  description = "Glue Catalog table name"
  type        = string
  default     = "video_advertisement"
}

variable "data_bucket" {
  description = "S3 bucket containing Parquet files"
  type        = string
}

variable "results_bucket" {
  description = "S3 bucket for Athena query results"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
