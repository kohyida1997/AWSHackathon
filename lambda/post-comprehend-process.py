import json
import os
import tarfile
import urllib
from io import BytesIO

import boto3
import pandas as pd


def get_docname_prefix_from_doc_topics_output(df):
    idx_to_split = df['docname'].loc[0].find(':')
    return df['docname'].loc[0][:idx_to_split + 1]


def get_docname_added_csv(df, docname_prefix):
    df['docname'] = docname_prefix + df.index.astype(str)
    return df


def create_enriched_csv_with_text_timestamp_and_topics(text_timestamps, topics_output):
    docname_prefix = get_docname_prefix_from_doc_topics_output(topics_output)
    temp_csv_with_docnames = get_docname_added_csv(
        text_timestamps, docname_prefix)
    dominant_topic_key = 'dominant_topic'
    proportion_key = 'proportion'

    temp_csv_with_docnames[dominant_topic_key] = -1
    temp_csv_with_docnames[proportion_key] = -1

    for index, row in temp_csv_with_docnames.iterrows():
        docname_for_this_row = row['docname']
        id_max = topics_output[topics_output['docname'] == docname_for_this_row][[
            'proportion']].idxmax()
        dominant_topic_for_this_row = int(topics_output.loc[id_max]['topic'])
        proportion_for_this_dominant_topic = float(
            topics_output.loc[id_max]['proportion'])
        temp_csv_with_docnames.at[index,
                                  dominant_topic_key] = dominant_topic_for_this_row
        temp_csv_with_docnames.at[index,
                                  proportion_key] = proportion_for_this_dominant_topic
    return temp_csv_with_docnames


def process_topics_output(s3_client, src_bucket, gzipped_key, dest_bucket, dest_key, cleaned_bucket):
    input_tar_file = s3_client.get_object(Bucket=src_bucket, Key=gzipped_key)
    input_tar_content = input_tar_file['Body'].read()

    with tarfile.open(fileobj=BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if (tar_resource.isfile()):
                inner_file_bytes = tar.extractfile(tar_resource).read()
                key = dest_key + "-" + tar_resource.name
                topics_output = pd.read_csv(BytesIO(inner_file_bytes))

                if tar_resource.name.find('doc-topics') > -1:
                    docname_prefix = get_docname_prefix_from_doc_topics_output(
                        topics_output)
                    print(docname_prefix)
                    text_timestamps_key = 'timestamp-' + docname_prefix
                    text_timestamps = pd.read_csv(BytesIO(s3_client.get_object(
                        Bucket=cleaned_bucket, Key=text_timestamps_key)['Body'].read()))
                    text_timestamps_docname = get_docname_added_csv(
                        text_timestamps, docname_prefix)
                    final_output = create_enriched_csv_with_text_timestamp_and_topics(
                        text_timestamps, topics_output)
                else:
                    final_output = pd.read_csv(BytesIO(inner_file_bytes))

                s3_client.put_object(Body=final_output.to_csv(
                    index=False), Bucket=dest_bucket, Key=key, ACL='bucket-owner-full-control')


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
    cleaned_bucket = os.environ['CLEAN_BUCKET']
    processed_key_topics = "TOPICS-output-" + job_id
    processed_key_sentiment = "SENTIMENT-output-" + job_id
    score_types = ["Mixed", "Negative", "Neutral", "Positive"]

    # initialize s3 client
    s3_client = boto3.client('s3')

    if gzipped_key.find('TOPICS') > -1:
        process_topics_output(s3_client, raw_bucket, gzipped_key,
                            processed_bucket, processed_key_topics, cleaned_bucket) 
    elif gzipped_key.find('SENTIMENT') > -1:
        process_sentiment_output(s3_client, raw_bucket, gzipped_key,
                                 score_types, processed_bucket, processed_key_sentiment)

    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Hello from Lambda!')
    # }
