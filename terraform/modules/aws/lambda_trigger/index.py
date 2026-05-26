import json
import boto3
import os

glue = boto3.client('glue')

def lambda_handler(event, context):
    """
    Trigger a Glue Job when called by EventBridge
    """
    job_name = os.environ['GLUE_JOB_NAME']
    
    try:
        response = glue.start_job_run(
            JobName=job_name
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Glue Job {job_name} started successfully',
                'jobRunId': response['JobRunId']
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
