import pandas as pd


def process_sentiment_output(s3_client, src_bucket, src_key, score_types, dest_bucket, dest_key):
    response = s3_client.get_object(Bucket=src_bucket, Key=src_key)
    body = response['Body']

    data = pd.read_json(body, lines=True)

    for t in score_types:
        data['SentimentScore'].apply(
            lambda x: x['Positive'] if type(x) == dict else 0)

    data.drop(columns=['SentimentScore', 'File'], inplace=True)

    if 'ErrorCode' in data.columns:
        data.drop(columns='ErrorCode', inplace=True)

    if 'ErrorMessage' in data.columns:
        data.drop(columns='ErrorMessage', inplace=True)

    s3_client.put_object(ACL='bucket-owner-full-control',
                  Body=data.to_csv(), Bucket=dest_bucket, Key=dest_key)


def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # setup constants
    src_bucket = 'hackathon-comprehend-raw-results'
    src_key = 'SentimentProcessedOutput' #TODO standardize naming
    score_types = ["Mixed", "Negative", "Neutral", "Positive"]
    dest_bucket = 'hackathon-comprehend-processed-results'
    dest_key = 'ProcessedOutput.csv' #TODO standardize naming

    process_sentiment_output(s3, src_bucket, src_key, score_types, dest_bucket, dest_key)
