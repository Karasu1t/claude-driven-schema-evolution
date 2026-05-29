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
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType, LongType
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Parse arguments - support both Phase 2 (Parquet) and Phase 3 (Iceberg)
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_BUCKET',
    'OUTPUT_BUCKET',
])

# Optional Iceberg arguments (Phase 3)
iceberg_enabled = False
iceberg_warehouse = None
iceberg_database = None
iceberg_table_name = None

if '--iceberg_warehouse' in sys.argv:
    iceberg_enabled = True
    idx = sys.argv.index('--iceberg_warehouse')
    iceberg_warehouse = sys.argv[idx + 1]
    
if '--glue_database' in sys.argv:
    idx = sys.argv.index('--glue_database')
    iceberg_database = sys.argv[idx + 1]
    
if '--iceberg_table_name' in sys.argv:
    idx = sys.argv.index('--iceberg_table_name')
    iceberg_table_name = sys.argv[idx + 1]

# Handle optional VER_DATE argument (for date-based file processing)
ver_date_formatted = None
if '--VER_DATE' in sys.argv:
    idx = sys.argv.index('--VER_DATE')
    if idx + 1 < len(sys.argv):
        ver_date_raw = sys.argv[idx + 1]
        try:
            from datetime import datetime as dt
            date_obj = dt.strptime(ver_date_raw, '%Y%m%d')
            ver_date_formatted = date_obj.strftime('%Y-%m-%d')
            logger.info(f"Converted VER_DATE from {ver_date_raw} to {ver_date_formatted}")
        except ValueError as e:
            logger.warning(f"Failed to parse VER_DATE {ver_date_raw}: {str(e)}")

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def configure_iceberg_catalog(spark_session):
    """
    Configure Iceberg catalog for Glue execution
    Attempts to set up GlueCatalog for native Iceberg support
    """
    try:
        logger.info("Configuring Iceberg catalog...")
        
        # Set Iceberg catalog configurations
        spark_session.conf.set("spark.sql.catalog.glue_catalog", 
                              "org.apache.iceberg.spark.SparkCatalog")
        spark_session.conf.set("spark.sql.catalog.glue_catalog.warehouse", 
                              iceberg_warehouse)
        spark_session.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", 
                              "org.apache.iceberg.aws.glue.GlueCatalog")
        spark_session.conf.set("spark.sql.catalog.glue_catalog.io-impl", 
                              "org.apache.iceberg.aws.s3.S3FileIO")
        
        logger.info("Iceberg catalog configuration applied")
        return True
    except Exception as e:
        logger.warning(f"Iceberg catalog configuration failed: {str(e)}")
        return False

# Attempt Iceberg configuration if enabled
iceberg_configured = False
if iceberg_enabled:
    iceberg_configured = configure_iceberg_catalog(spark)
    if not iceberg_configured:
        logger.warning("Falling back to Parquet output")
        iceberg_enabled = False

try:
    logger.info(f"Starting Glue Job: {args['JOB_NAME']}")
    logger.info(f"Iceberg enabled: {iceberg_enabled}, Iceberg configured: {iceberg_configured}")
    
    # Read CSV from S3 bucket (with wildcard to match all CSV files)
    input_path = f"s3://{args['INPUT_BUCKET']}/*.csv"
    logger.info(f"Reading CSV from: {input_path}")
    
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True,
        mode="PERMISSIVE"
    )
    
    logger.info(f"Input records: {df.count()}")
    
    # Schema evolution: Ensure all required columns exist
    required_columns = [
        'video_title', 
        'views', 
        'channel_name', 
        'channel_subscribers', 
        'likes', 
        'video_duration_minutes'
    ]
    
    # Add missing columns
    for col_name in required_columns:
        if col_name not in df.columns:
            logger.info(f"Adding missing column: {col_name}")
            df = df.withColumn(col_name, lit(None).cast("string"))
    
    # Reorder columns
    df = df.select(required_columns)
    
    # Add processing metadata
    df = df.withColumn("processed_at", lit(datetime.now().isoformat()).cast("string"))
    df = df.withColumn("glue_job_run_id", lit(args['JOB_NAME']).cast("string"))
    
    # Add ver_date column if provided
    if ver_date_formatted:
        logger.info(f"Adding ver_date column: {ver_date_formatted}")
        df = df.withColumn("ver_date", lit(ver_date_formatted).cast("string"))
    else:
        logger.warning("VER_DATE not provided or failed to parse, skipping ver_date column")
    
    logger.info(f"Output records: {df.count()}")
    
    # Write output - Iceberg or Parquet
    if iceberg_enabled and iceberg_configured and iceberg_database and iceberg_table_name:
        try:
            logger.info(f"Writing to Iceberg table: {iceberg_database}.{iceberg_table_name}")
            
            # Create temporary view for SQL operations
            df.createOrReplaceTempView("temp_video_data")
            
            # Use SQL to write to Iceberg (more reliable than DataFrame API)
            table_location = f"{iceberg_warehouse}/{iceberg_table_name}"
            sql_query = f"""
                CREATE OR REPLACE TABLE glue_catalog.{iceberg_database}.{iceberg_table_name}
                USING iceberg
                LOCATION '{table_location}'
                AS SELECT * FROM temp_video_data
            """
            
            logger.info(f"Executing SQL: {sql_query}")
            spark.sql(sql_query)
            logger.info(f"Successfully wrote {df.count()} records to Iceberg table")
            
        except Exception as e:
            logger.error(f"Iceberg write failed: {str(e)}")
            logger.warning("Falling back to Parquet output")
            
            # Fallback to Parquet
            output_path = f"s3://{args['OUTPUT_BUCKET']}/processed_data"
            logger.info(f"Writing to Parquet: {output_path}")
            df.write \
                .option("compression", "snappy") \
                .mode("overwrite") \
                .format("parquet") \
                .save(output_path)
            logger.info(f"Fallback: wrote {df.count()} records to Parquet")
    else:
        # Standard Parquet output (Phase 2)
        output_path = f"s3://{args['OUTPUT_BUCKET']}/processed_data"
        logger.info(f"Writing to Parquet: {output_path}")
        
        df.write \
            .option("compression", "snappy") \
            .mode("overwrite") \
            .format("parquet") \
            .save(output_path)
        
        logger.info(f"Successfully wrote {df.count()} records to Parquet at {output_path}")
    
    logger.info("Schema evolution processing completed successfully")
    job.commit()

except Exception as e:
    logger.error(f"Error in Glue Job: {str(e)}", exc_info=True)
    job.commit()
    raise
