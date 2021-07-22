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
                key = dest_key + "-" + tar_resource.name
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
                    data[t] = data['SentimentScore'].apply(
                        lambda x: x[t] if type(x) == dict else 0)
            
                data.drop(columns=['SentimentScore', 'File'], inplace=True)
            
                if 'ErrorCode' in data.columns:
                    data.drop(columns='ErrorCode', inplace=True)
            
                if 'ErrorMessage' in data.columns:
                    data.drop(columns='ErrorMessage', inplace=True)
            
                s3_client.put_object(ACL='bucket-owner-full-control',
                                     Body=data.to_csv(), Bucket=dest_bucket, Key=dest_key + '.csv')

def get_job_id(key):
    res = ""
    if key.find('TOPICS') > -1:
        loc = key.find('TOPICS') + len("TOPICS") + 1
        while key[loc] != '/':
            res = res + key[loc]
            loc = loc + 1
    elif key.find('SENTIMENT') > -1:
        loc = key.find('SENTIMENT') + len("SENTIMENT") + 1
        while key[loc] != '/': 
            res = res + key[loc]
            loc = loc + 1
    return res
    

def lambda_handler(event, context):
    raw_bucket = os.environ['SRC_BUCKET']
    gzipped_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # setup constants
    
    job_id = get_job_id(gzipped_key)
    print(gzipped_key, job_id)
    processed_bucket = os.environ['DEST_BUCKET']
    processed_key_topics = "TOPICS-output-" + job_id
    processed_key_sentiment = "SENTIMENT-output-" + job_id
    score_types = ["Mixed", "Negative", "Neutral", "Positive"]

    # initialize s3 client
    s3_client = boto3.client('s3')

    if gzipped_key.find('TOPICS') > -1:
        unzip_topics_output(s3_client, raw_bucket, gzipped_key,
                            processed_bucket, processed_key_topics)
    elif gzipped_key.find('SENTIMENT') > -1:
        process_sentiment_output(s3_client, raw_bucket, gzipped_key,
                                 score_types, processed_bucket, processed_key_sentiment)

    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Hello from Lambda!')
    # }
