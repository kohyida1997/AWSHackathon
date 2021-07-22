import json
import boto3
from io import BytesIO
import tarfile
import os

def lambda_handler(event, context):
    # src_bucket = event['Records'][0]['s3']['bucket']['name']
    # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # setup constants
    job_id = event['JobId']
    src_bucket = "comprehend-processed-data"
    dest_bucket = os.environ['DEST_BUCKET']
    gzipped_key = "322895421085-TOPICS-" + job_id + "/output/output.tar.gz"
    uncompressed_key = "TOPICS-output-" + job_id 
    
    # initialize s3 client
    s3_client = boto3.client('s3') 
    input_tar_file = s3_client.get_object(Bucket=src_bucket, Key=gzipped_key)
    input_tar_content = input_tar_file['Body'].read()
    
    with tarfile.open(fileobj = BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if (tar_resource.isfile()):
                inner_file_bytes = tar.extractfile(tar_resource).read()
                key = uncompressed_key + tar_resource.name
                s3_client.upload_fileobj(BytesIO(inner_file_bytes), Bucket=dest_bucket, Key=key)
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }



