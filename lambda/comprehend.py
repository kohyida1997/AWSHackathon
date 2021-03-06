import boto3
import json
import urllib

def start_topic_modelling(comprehend, input_data_config, output_data_config, data_access_role_arn, number_of_topics, job_name):
     
    start_topics_detection_job_result = comprehend.start_topics_detection_job(NumberOfTopics=number_of_topics,
                                                                              InputDataConfig=input_data_config,
                                                                              OutputDataConfig=output_data_config,
                                                                              DataAccessRoleArn=data_access_role_arn,
                                                                              JobName=job_name)
     
    print('start_topics_detection_job_result: ', json.dumps(start_topics_detection_job_result))
     
    job_id = start_topics_detection_job_result["JobId"]
    print('job_id: ' + job_id)

    return job_id


def start_sentiment_analysis(comprehend, input_data_config, output_data_config, data_access_role_arn, number_of_topics, job_name):

    start_sentiment_detection_job_result = comprehend.start_sentiment_detection_job(InputDataConfig=input_data_config,
                                                                                    OutputDataConfig=output_data_config,
                                                                                    DataAccessRoleArn=data_access_role_arn,
                                                                                    LanguageCode='en',
                                                                                    JobName=job_name)

    print('start_sentiment_detection_job_result: ',
          json.dumps(start_sentiment_detection_job_result))

    job_id = start_sentiment_detection_job_result["JobId"]
    print('job_id: ' + job_id)

    return job_id
    
def lambda_handler(event, context):
    # if "Filename" not in event: 
    #     return {
    #         'statusCode': 400,
    #         'body': json.dumps('No file!')
    #     }
    data_access_role_arn = 'arn:aws:iam::322895421085:role/comprehend-s3-access'
    comprehend = boto3.client(service_name='comprehend', region_name='ap-southeast-1')
    s3 = boto3.client('s3')
    filename = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    input_s3_url = "s3://hackathon-cleaned-data/" + filename
    print(input_s3_url)
    input_doc_format = "ONE_DOC_PER_LINE"
    output_s3_url = "s3://hackathon-comprehend-raw-results"
    number_of_topics = 10

    input_data_config = {"S3Uri": input_s3_url, "InputFormat": input_doc_format}
    output_data_config = {"S3Uri": output_s3_url}

    topic_job_name = "Topic Modelling Job"
    sentiment_job_name = "Sentiment Analysis Job"
    
    # if event["Topic"]: 
    topic_job_id = start_topic_modelling(
        comprehend, input_data_config, output_data_config, data_access_role_arn, number_of_topics, topic_job_name)
    describe_topics_detection_job_result = comprehend.describe_topics_detection_job(JobId=topic_job_id)
    print('describe_topics_detection_job_result: ', describe_topics_detection_job_result)
        
    # if event["Sentiment"]:
    sentiment_job_id = start_sentiment_analysis(
        comprehend, input_data_config, output_data_config, data_access_role_arn, number_of_topics, sentiment_job_name)
    describe_sentiment_detection_job_result = comprehend.describe_sentiment_detection_job(JobId=sentiment_job_id)
    print('describe_sentiment_detection_job_result: ', describe_sentiment_detection_job_result)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Job started!')
    }