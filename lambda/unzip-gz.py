import json
import os
import tarfile
import urllib
from io import BytesIO

import boto3
import pandas as pd


def unzip_topics_output(s3_client, src_bucket, gzipped_key, dest_bucket, dest_key):
    input_tar_file = s3_client.get_object(Bucket=src_bucket, Key=gzipped_key)
    input_tar_content = input_tar_file['Body'].read()

    with tarfile.open(fileobj=BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if (tar_resource.isfile()):
                inner_file_bytes = tar.extractfile(tar_resource).read()
                key = dest_key + tar_resource.name
                s3_client.upload_fileobj(
                    BytesIO(inner_file_bytes), Bucket=dest_bucket, Key=key)


def process_sentiment_output(s3_client, src_bucket, gzipped_key, score_types, dest_bucket, dest_key):

    input_tar_file = s3_client.get_object(Bucket=src_bucket, Key=gzipped_key)
    input_tar_content = input_tar_file['Body'].read()

    with tarfile.open(fileobj=BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if (tar_resource.isfile()):
                inner_file_bytes = tar.extractfile(tar_resource).read()

    data = pd.read_json(inner_file_bytes, lines=True)

    for t in score_types:
        data['SentimentScore'].apply(
            lambda x: x['Positive'] if type(x) == dict else 0)

    data.drop(columns=['SentimentScore', 'File'], inplace=True)

    if 'ErrorCode' in data.columns:
        data.drop(columns='ErrorCode', inplace=True)

    if 'ErrorMessage' in data.columns:
        data.drop(columns='ErrorMessage', inplace=True)

    s3_client.put_object(ACL='bucket-owner-full-control',
                         Body=data.to_csv(), Bucket=dest_bucket, Key=dest_key+tar_resource.name)


def lambda_handler(event, context):
    # raw_bucket = event['Records'][0]['s3']['bucket']['name']
    gzipped_key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # setup constants
    job_id = event['JobId']
    raw_bucket = "hackathon-comprehend-raw-results"
    # processed_bucket = os.environ['DEST_BUCKET']
    processed_bucket = "hackathon-comprehend-processed-results"
    # gzipped_key = "322895421085-TOPICS-" + job_id + "/output/output.tar.gz"
    processed_key_topics = "TOPICS-output-" + job_id
    processed_key_sentiment = "SENTIMENT-output-" + job_id
    score_types = ["Mixed", "Negative", "Neutral", "Positive"]

    # initialize s3 client
    s3_client = boto3.client('s3')

    if gzipped_key.find('TOPICS') > -1:
        unzip_topics_output(s3_client, raw_bucket, gzipped_key,
                            processed_bucket, processed_key_topics)
    elif gzipped_key.find('SENTIMENTS') > -1:
        process_sentiment_output(s3_client, raw_bucket, gzipped_key,
                                 score_types, processed_bucket, processed_key_sentiment)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
