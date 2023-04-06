try:
    import unzip_requirements as unzip_requirements
except ImportError:
    pass

import json
import os
import urllib.parse
from botocore.exceptions import ClientError
from boto3 import Session
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

from src.utils.logger import logger_config
logger = logger_config()

region_name = os.getenv('REGION_NAME')
job_name = os.getenv('JOB_NAME')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
  
    logger.info(bucket)
    session = Session(region_name=region_name)
    
    try: 
        client = session.client('glue')
        response = client.start_job_run(JobName=job_name, Arguments={'--region_name': region_name, '--bucket_name': bucket, '--path_suffix': key})

        if response['ResponseMetadata']['HTTPStatusCode'] >=200 or response['ResponseMetadata']['HTTPStatusCode'] <= 204 :
            logger.info("The Job started successfully!")

    except ClientError as e:
        logger.exception(e)

        raise Exception(json.dumps({
            'exception_type': 'boto3 Exception',
            'message': str(e),
            'status_code': e.response['ResponseMetadata']['HTTPStatusCode'],
            'code': e.response['Error']['Code']
        }))
    
    except Exception as e:
        logger.exception(e)
        raise Exception(json.dumps({
            'message': str(e),
            'status_code':500,
            'code': 'UnexpectedError'
        }))