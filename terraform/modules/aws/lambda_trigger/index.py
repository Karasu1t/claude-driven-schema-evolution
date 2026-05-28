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

def format_date_for_iceberg(date_yyyymmdd: str) -> str:
    """
    Convert yyyymmdd format to YYYY/MM/DD for Iceberg input path resolution
    Example: '20260528' -> '2026/05/28'
    """
    if not date_yyyymmdd or len(date_yyyymmdd) != 8:
        return "AUTO"  # Use automatic date resolution if invalid
    try:
        year = date_yyyymmdd[0:4]
        month = date_yyyymmdd[4:6]
        day = date_yyyymmdd[6:8]
        return f"{year}/{month}/{day}"
    except Exception:
        return "AUTO"

def lambda_handler(event, context):
    """
    Trigger a Glue Job for Iceberg table processing
    
    Handles:
    - S3 PUT events from EventBridge/S3
    - Manual trigger via EventBridge schedule
    
    Extracts date from filename and passes to Glue Job in YYYY/MM/DD format
    """
    job_name = os.environ['GLUE_JOB_NAME']
    
    try:
        date_yyyymmdd = None
        
        # Try to extract date from S3 event
        if 'Records' in event and len(event['Records']) > 0:
            # S3 event
            record = event['Records'][0]
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            filename = key.split('/')[-1]
            
            date_yyyymmdd = extract_date_from_filename(filename)
            if date_yyyymmdd:
                print(f"Extracted date from {filename}: {date_yyyymmdd}")
        
        # If no date extracted, use today's date (yyyymmdd format)
        if not date_yyyymmdd:
            today = datetime.now().strftime('%Y%m%d')
            date_yyyymmdd = today
            print(f"No date in filename, using today: {date_yyyymmdd}")
        
        # Convert date format for Iceberg input path resolution
        target_date = format_date_for_iceberg(date_yyyymmdd)
        
        # Build arguments for Iceberg Glue Job
        # --target_date: date in YYYY/MM/DD or AUTO format (for Iceberg path resolution)
        arguments = {
            '--target_date': target_date
        }
        
        # Start Glue Job with dynamic arguments
        response = glue.start_job_run(
            JobName=job_name,
            Arguments=arguments
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Glue Job {job_name} started successfully',
                'jobRunId': response['JobRunId'],
                'targetDate': target_date,
                'extractedDate': date_yyyymmdd
            })
        }
    except Exception as e:
        print(f'Error starting Glue Job: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Failed to start Glue Job: {str(e)}'
            })
        }
