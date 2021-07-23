import logging
import boto3
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')

def put_object(body, bucket, key):
    try:
        response = s3_client.put_object(ACL='bucket-owner-full-control', Body=body, Bucket=bucket, Key=key)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_fileobj(fileobj, bucket, key, extraArgs=None, callback=None, config=None):
    try:
        response = s3_client.upload_fileobj(fileobj, bucket, key, extraArgs, callback, config)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True