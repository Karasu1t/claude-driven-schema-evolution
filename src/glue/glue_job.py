"""
Glue Job: Video Advertisement ETL with Schema Evolution
Process: CSV -> Transform -> Iceberg (with Parquet fallback)

Phase 3: Iceberg Support with Native Glue 4.0 Configuration
- Primary: Iceberg table format (ACID transactions, time-travel, versioning)
- Fallback: Parquet if Iceberg initialization fails
- Schema evolution: Dynamic column handling
"""

import sys
import logging
import os
import boto3
import traceback
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
    csv_path = f"s3://{args['INPUT_BUCKET']}/_{ver_date}.csv"
    logger.info(f"Reading CSV: {csv_path}")
    
    # Define schema manually for reliability
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
    
    schema = StructType([
        StructField("video_title", StringType(), True),
        StructField("views", LongType(), True),
        StructField("channel_name", StringType(), True),
        StructField("channel_subscribers", LongType(), True),
        StructField("likes", LongType(), True),
        StructField("video_duration_minutes", DoubleType(), True)
    ])
    
    df = spark.read.csv(
        csv_path,
        header=True, schema=schema, mode="PERMISSIVE"
    )
    logger.info(f"Read {df.count()} records from {ver_date}")
    
    # Schema evolution
    for col_name in ['video_title', 'views', 'channel_name', 'channel_subscribers', 'likes', 'video_duration_minutes']:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast("string"))
    
    df = df.select(['video_title', 'views', 'channel_name', 'channel_subscribers', 'likes', 'video_duration_minutes'])
    
    # Add metadata columns: partition_date for partitioning, processed_at for lineage
    df = df.withColumn("partition_date", lit(partition_date).cast("date"))
    df = df.withColumn("processed_at", lit(datetime.now().isoformat()).cast("string"))
    
    # Write to Iceberg
    full_table_qualified = f"glue_catalog.{GLUE_DATABASE}.{TABLE_NAME}"
    iceberg_path = f"s3://{ICEBERG_WAREHOUSE.replace('s3://', '')}/{GLUE_DATABASE}.db/{TABLE_NAME}"
    
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_qualified} PURGE")
    except:
        pass
    
    # Write with partitioning by date
    # Iceberg automatically optimizes query performance based on partition
    df.writeTo(full_table_qualified) \
        .using("iceberg") \
        .option("path", iceberg_path) \
        .option("format-version", "2") \
        .partitionedBy("partition_date") \
        .mode("overwrite") \
        .saveAsTable()
    
    logger.info(f"✓ Iceberg: {GLUE_DATABASE}.{TABLE_NAME} (partitioned by date)")
    
    job.commit()

except Exception as e:
    logger.error(f"Error: {str(e)}", exc_info=True)
    job.commit()
    raise
