# # Used when Snowflake remote state has no outputs yet (e.g. empty state or apply order).
# # Prefer applying env/dev/snowflake first so outputs come from terraform_remote_state.

# variable "snowflake_storage_integration_user_arn" {
#   type        = string
#   default     = ""
#   description = "Fallback STORAGE_AWS_IAM_USER_ARN when Snowflake state outputs are empty; from DESCRIBE STORAGE INTEGRATION."
# }

# variable "snowflake_storage_integration_external_id" {
#   type        = string
#   default     = ""
#   sensitive   = true
#   description = "Fallback STORAGE_AWS_EXTERNAL_ID when Snowflake state outputs are empty; from DESCRIBE STORAGE INTEGRATION."
# }

# variable "snowflake_glue_catalog_user_arn" {
#   type        = string
#   default     = ""
#   description = "GLUE_AWS_IAM_USER_ARN from DESC CATALOG INTEGRATION (not the same as STORAGE). Required on the same IAM role trust for Iceberg + Glue."
# }

# variable "snowflake_glue_catalog_external_id" {
#   type        = string
#   default     = ""
#   sensitive   = true
#   description = "GLUE_AWS_EXTERNAL_ID from DESC CATALOG INTEGRATION."

#   validation {
#     condition = (
#       (var.snowflake_glue_catalog_user_arn == "" && var.snowflake_glue_catalog_external_id == "") ||
#       (length(trimspace(var.snowflake_glue_catalog_user_arn)) > 0 && length(trimspace(var.snowflake_glue_catalog_external_id)) > 0)
#     )
#     error_message = "Set both snowflake_glue_catalog_user_arn and snowflake_glue_catalog_external_id, or leave both empty."
#   }
# }

# variable "snowflake_external_volume_external_id" {
#   type        = string
#   default     = ""
#   sensitive   = true
#   description = "Fallback STORAGE_AWS_EXTERNAL_ID from DESC EXTERNAL VOLUME (S3 location) when Snowflake state has no output yet."
# }

# variable "snowflake_external_volume_user_arn" {
#   type        = string
#   default     = ""
#   description = "Fallback STORAGE_AWS_IAM_USER_ARN for EXTERNAL VOLUME S3 location; often same as STORAGE INTEGRATION user."
# }
