##############################################
# S3
##############################################
module "s3_lambda_raw_bucket" {
  source      = "../../../modules/aws/s3_raw_bucket"
  project     = local.project
  environment = local.environment

  tags = {
    Component = "Storage"
  }
}

module "s3_lambda_processed_bucket" {
  source      = "../../../modules/aws/s3_processed_bucket"
  project     = local.project
  environment = local.environment

  tags = {
    Component = "Storage"
  }
}

##############################################
# Glue Job for Schema Evolution
##############################################
module "glue_etl_job" {
  source = "../../../modules/aws/glue_job"

  job_name                = "schema-evolution-etl"
  environment             = local.environment
  project                 = local.project
  input_bucket            = module.s3_lambda_raw_bucket.bucket_name
  output_bucket           = module.s3_lambda_processed_bucket.bucket_name
  script_location         = "s3://${module.s3_lambda_processed_bucket.bucket_name}/scripts/glue_job.py"
  glue_script_source_path = abspath("${path.module}/../../../../src/glue/glue_job.py")

  worker_type     = "G.2X"
  num_workers     = 2
  timeout_minutes = 30
  tags = {
    Component = "ETL"
    Purpose   = "Schema Evolution"
  }
}

##############################################
# Lambda function to trigger Glue Job
##############################################
module "lambda_trigger" {
  source = "../../../modules/aws/lambda_trigger"

  function_name = "glue-job-trigger"
  environment   = local.environment
  project       = local.project
  glue_job_name = module.glue_etl_job.glue_job_name

  tags = {
    Component = "Orchestration"
    Purpose   = "Glue Job Trigger"
  }
}

##############################################
# EventBridge for scheduled Lambda invocation
##############################################
module "eventbridge_trigger" {
  source = "../../../modules/aws/eventbridge"

  environment         = local.environment
  project             = local.project
  lambda_function_arn = module.lambda_trigger.lambda_function_arn

  tags = {
    Component = "Orchestration"
    Schedule  = "Daily at 6 AM UTC"
  }
}

##############################################
# Athena + Glue Catalog for Iceberg tables
##############################################
module "athena_iceberg" {
  source = "../../../modules/aws/athena"

  environment           = local.environment
  project               = local.project
  athena_results_bucket = module.s3_lambda_processed_bucket.bucket_name

  tags = {
    Component = "Query"
    Purpose   = "Iceberg Table Analysis"
  }
}

##############################################
# Phase 4: Glue Catalog Table + Athena Integration
##############################################
# DISABLED: Iceberg table created via Python SQL in glue_job.py
# Terraform-managed Parquet table was blocking Iceberg creation with type=null error
# Let Python script manage Iceberg table creation instead
/*
module "glue_table_registration" {
  source = "../../../modules/aws/glue_table"

  environment    = local.environment
  project        = local.project
  glue_database  = module.athena_iceberg.glue_database_name
  table_name     = "video_advertisement"
  data_bucket    = module.s3_lambda_processed_bucket.bucket_name
  results_bucket = module.s3_lambda_processed_bucket.bucket_name

  tags = {
    Component = "Query"
    Purpose   = "Parquet Data Lake + Athena"
    Phase     = "4"
  }

  depends_on = [
    module.glue_etl_job,
    module.athena_iceberg
  ]
}
*/
