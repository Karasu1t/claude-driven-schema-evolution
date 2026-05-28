"""
Glue Job: Video Advertisement ETL with Schema Evolution
Process: CSV -> Transform -> Parquet
"""

import sys
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, col
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Parse arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_BUCKET',
    'OUTPUT_BUCKET',
])

# Handle optional VER_DATE argument
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

try:
    logger.info(f"Starting Glue Job: {args['JOB_NAME']}")
    
    # Read CSV from S3 bucket (with wildcard to match all CSV files)
    input_path = f"s3://{args['INPUT_BUCKET']}/*.csv"
    logger.info(f"Reading CSV from: {input_path}")
    
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True,
        mode="PERMISSIVE"
    )
    
    logger.info(f"Input schema: {df.printSchema()}")
    logger.info(f"Record count: {df.count()}")
    
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
    
    logger.info(f"Final schema: {df.printSchema()}")
    
    # Write to Parquet in processed bucket
    output_path = f"s3://{args['OUTPUT_BUCKET']}/processed_data"
    logger.info(f"Writing Parquet to: {output_path}")
    
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
