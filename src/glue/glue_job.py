"""
Glue Job: Video Platform ETL with Schema Evolution
Process: CSV -> Transform -> Iceberg/Parquet
"""

import sys
import logging
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    logger.info(f"Starting Glue Job: {args['JOB_NAME']}")
    
    # 1. Read CSV from S3 raw bucket
    input_path = f"s3://{args['INPUT_BUCKET']}/raw/"
    logger.info(f"Reading CSV from: {input_path}")
    
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True,
        mode="PERMISSIVE"
    )
    
    logger.info(f"Input schema: {df.printSchema()}")
    logger.info(f"Record count: {df.count()}")
    
    # 2. Schema evolution: Ensure all required columns exist
    # Phase 0.5: video_title, views, channel_name, channel_subscribers, likes, video_duration_minutes
    required_columns = [
        'video_title', 
        'views', 
        'channel_name', 
        'channel_subscribers', 
        'likes', 
        'video_duration_minutes'
    ]
    
    # Add missing columns (will be filled by Claude in Phase 1)
    for col_name in required_columns:
        if col_name not in df.columns:
            logger.info(f"Adding missing column: {col_name}")
            df = df.withColumn(col_name, lit(None).cast("string"))
    
    # Reorder columns
    df = df.select(required_columns)
    
    # 3. Add processing metadata
    df = df.withColumn("processed_at", lit(datetime.now().isoformat()).cast("string"))
    df = df.withColumn("glue_job_run_id", lit(args['JOB_NAME']).cast("string"))
    
    logger.info(f"Final schema: {df.printSchema()}")
    
    # 4. Write to Parquet in processed bucket
    output_path = f"s3://{args['OUTPUT_BUCKET']}/processed/"
    logger.info(f"Writing Parquet to: {output_path}")
    
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(output_path)
    
    logger.info(f"Successfully wrote {df.count()} records to {output_path}")
    
    # 5. Register in Glue Catalog (optional)
    # This creates a metadata entry for the processed data
    output_dyf = DynamicFrame.fromDF(df, glueContext, "output")
    
    glueContext.write_dynamic_frame.from_options(
        frame=output_dyf,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": []
        },
        format="parquet",
        transformation_ctx="output"
    )
    
    logger.info("Schema evolution processing completed successfully")
    job.commit()

except Exception as e:
    logger.error(f"Error in Glue Job: {str(e)}", exc_info=True)
    job.commit()
    raise
