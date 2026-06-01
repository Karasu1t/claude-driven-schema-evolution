"""
Glue Job: Video Advertisement ETL with Schema Evolution
Process: CSV -> Transform -> Apache Iceberg (AWS Glue 4.0)

- Format : Iceberg v2 (mandatory — no fallback)
- Catalog : AWS Glue Catalog (GlueCatalog)
- Schema  : Single source of truth via SOURCE_SCHEMA / SOURCE_COLUMNS
- Evolution: Missing columns auto-filled with null; new columns added to SOURCE_SCHEMA only
"""

import sys
import logging
import os
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Iceberg configuration
# Note: Bucket names can be dynamic (prod or test)
# Defaults provided for backward compatibility
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_BUCKET', 'OUTPUT_BUCKET'])
input_bucket = args['INPUT_BUCKET']
output_bucket = args['OUTPUT_BUCKET']

logger.info(f"Input bucket: {input_bucket}")
logger.info(f"Output bucket: {output_bucket}")

# Determine if running in test mode (bucket contains 'test')
is_test_mode = 'test' in output_bucket.lower()
if is_test_mode:
    ICEBERG_WAREHOUSE = f"s3://{output_bucket}/iceberg-warehouse"
    GLUE_DATABASE = "dev_karasuit_iceberg_db_test"
    logger.info("TEST MODE: Using test database and warehouse")
else:
    ICEBERG_WAREHOUSE = f"s3://{output_bucket}/iceberg-warehouse"
    GLUE_DATABASE = "dev_karasuit_iceberg_db"
    logger.info("PRODUCTION MODE: Using production database and warehouse")

TABLE_NAME = "video_advertisement"

logger.info(f"Job: {args['JOB_NAME']}")

# Initialize Spark and Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Glue Iceberg Catalog
try:
    boto_session = boto3.session.Session()
    glue_region = boto_session.region_name or os.environ.get("AWS_REGION")
    glue_account = boto3.client("sts", region_name=glue_region).get_caller_identity()["Account"]
    
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
    spark.conf.set("spark.sql.catalog.glue_catalog.glue.id", glue_account)
    spark.conf.set("spark.sql.catalog.glue_catalog.glue.region", glue_region)
    spark._jsparkSession.sessionState().catalogManager().setCurrentCatalog("glue_catalog")
    logger.info(f"Iceberg catalog configured for {GLUE_DATABASE}.{TABLE_NAME}")
    
    # Create database if not exists
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {GLUE_DATABASE}"
    spark.sql(create_db_sql)
    logger.info(f"Database {GLUE_DATABASE} ready")
    
except Exception as e:
    logger.error(f"Catalog config failed: {str(e)}", exc_info=True)
    raise

try:
    # Get VER_DATE from arguments (required for file filtering)
    ver_date = None
    if '--VER_DATE' in sys.argv:
        idx = sys.argv.index('--VER_DATE')
        if idx + 1 < len(sys.argv):
            ver_date = sys.argv[idx + 1]
    
    if not ver_date:
        raise ValueError("VER_DATE argument is required")
    
    # Convert yyyymmdd to yyyy-mm-dd for partition
    # e.g., "20260529" -> "2026-05-29"
    if len(ver_date) == 8 and ver_date.isdigit():
        partition_date = f"{ver_date[:4]}-{ver_date[4:6]}-{ver_date[6:8]}"
        logger.info(f"Partition date: {partition_date}")
    else:
        raise ValueError(f"Invalid VER_DATE format: {ver_date}. Expected yyyymmdd")
    
    # Read CSV with specific date
    # Try multiple filename patterns
    csv_candidates = [
        f"s3://{args['INPUT_BUCKET']}/sns_advertisement_{ver_date}.csv",
        f"s3://{args['INPUT_BUCKET']}/_{ver_date}.csv"
    ]
    
    csv_path = None
    for candidate in csv_candidates:
        try:
            # Try to read just the header/first row
            probe_df = spark.read.csv(candidate, header=True)
            csv_path = candidate
            logger.info(f"Found CSV file: {csv_path}")
            break
        except Exception as probe_e:
            logger.debug(f"Candidate {candidate} not found")
            continue
    
    if not csv_path:
        csv_path = csv_candidates[1]  # Fallback to default pattern
        logger.info(f"Using fallback pattern: {csv_path}")
    
    logger.info(f"Reading CSV: {csv_path}")
    
    from schema import SOURCE_SCHEMA, SOURCE_COLUMNS

    try:
        df = spark.read.csv(
            csv_path,
            header=True, schema=SOURCE_SCHEMA, mode="PERMISSIVE", encoding="utf-8"
        )
        record_count = df.count()
        logger.info(f"Successfully read {record_count} records from CSV")
    except Exception as e:
        logger.error(f"Failed to read CSV: {str(e)}")
        raise

    for col_name in SOURCE_COLUMNS:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast("string"))

    df = df.select(SOURCE_COLUMNS)

    df = df.withColumn("partition_date", lit(partition_date).cast("date"))
    df = df.withColumn("processed_at", lit(datetime.now().isoformat()).cast("string"))

    full_table_qualified = f"glue_catalog.{GLUE_DATABASE}.{TABLE_NAME}"
    iceberg_path = f"s3://{ICEBERG_WAREHOUSE.replace('s3://', '')}/{GLUE_DATABASE}.db/{TABLE_NAME}"

    logger.info(f"Writing {record_count} records to {full_table_qualified}")
    logger.info(f"Columns: {df.columns}")

    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_qualified} PURGE")
        logger.info(f"Dropped existing table: {full_table_qualified}")
    except Exception as e:
        logger.warning(f"Could not drop table: {e}")
    
    try:
        df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .option("path", iceberg_path) \
            .option("format-version", "2") \
            .partitionBy("partition_date") \
            .option("write.parquet.compression-codec", "snappy") \
            .saveAsTable(full_table_qualified)
        logger.info(f"✓ Successfully wrote to Iceberg: {full_table_qualified}")
    except Exception as e:
        logger.error(f"✗ Failed to write to Iceberg: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise
    
    logger.info(f"✓ Iceberg: {GLUE_DATABASE}.{TABLE_NAME} (partitioned by date)")
    
    job.commit()

except Exception as e:
    logger.error(f"Error: {str(e)}", exc_info=True)
    job.commit()
    raise
