import json
import boto3
import os
import re
from datetime import datetime

glue = boto3.client('glue')

def extract_date_from_filename(filename):
    """
    Extract yyyymmdd from filename like 'sns_advertisement_20260528.csv'
    Returns: '20260528' or None if not found
    """
    match = re.search(r'(\d{8})\.csv$', filename)
    return match.group(1) if match else None

def lambda_handler(event, context):
    """
    Trigger a Glue Job for Iceberg processing
    
    Passes:
    - --VER_DATE: yyyymmdd format extracted from filename
    - --INPUT_BUCKET: S3 bucket for input CSV (can be overridden by env var)
    - --OUTPUT_BUCKET: S3 bucket for output/Iceberg (can be overridden by env var)
    """
    job_name = os.environ['GLUE_JOB_NAME']
    
    # Get bucket names from environment (can be overridden for test mode)
    input_bucket = os.environ.get('INPUT_BUCKET', 'dev-karasuit-raw-bucket')
    output_bucket = os.environ.get('OUTPUT_BUCKET', 'dev-karasuit-processed-bucket')
    
    logger.info(f"Using INPUT_BUCKET: {input_bucket}")
    logger.info(f"Using OUTPUT_BUCKET: {output_bucket}")
    
    try:
        ver_date_raw = None
        
        # Try to extract date from S3 event
        if 'Records' in event and len(event['Records']) > 0:
            # S3 event
            record = event['Records'][0]
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            filename = key.split('/')[-1]
            
            ver_date_raw = extract_date_from_filename(filename)
            if ver_date_raw:
                print(f"Extracted date from {filename}: {ver_date_raw}")
        
        # If no date extracted, use today's date (yyyymmdd format)
        if not ver_date_raw:
            today = datetime.now().strftime('%Y%m%d')
            ver_date_raw = today
            print(f"No date in filename, using today: {ver_date_raw}")
        
        # Build arguments including bucket names for dynamic configuration
        arguments = {
            '--VER_DATE': ver_date_raw,
            '--INPUT_BUCKET': input_bucket,
            '--OUTPUT_BUCKET': output_bucket
        }
        
        # Start Glue Job with dynamic arguments
        response = glue.start_job_run(
            JobName=job_name,
            Arguments=arguments
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Glue Job {job_name} started with Iceberg',
                'jobRunId': response['JobRunId'],
                'verDate': ver_date_raw,
                'inputBucket': input_bucket,
                'outputBucket': output_bucket
            })
        }
    except Exception as e:
        logger.error(f'Error starting Glue Job: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Failed to start Glue Job: {str(e)}'
            })
        }

