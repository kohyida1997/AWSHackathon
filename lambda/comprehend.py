import boto3
import json


def start_topic_modelling(comprehend):
    input_s3_url = "s3://comprehend-raw-data/Questions.csv"
    input_doc_format = "ONE_DOC_PER_LINE"
    output_s3_url = "s3://comprehend-processed-data/"
    data_access_role_arn = "arn:aws:iam::322895421085:role/comprehend-s3-access"
    number_of_topics = 10
     
    input_data_config = {"S3Uri": input_s3_url, "InputFormat": input_doc_format}
    output_data_config = {"S3Uri": output_s3_url}
     
    start_topics_detection_job_result = comprehend.start_topics_detection_job(NumberOfTopics=number_of_topics,
                                                                              InputDataConfig=input_data_config,
                                                                              OutputDataConfig=output_data_config,
                                                                              DataAccessRoleArn=data_access_role_arn)
                                                                            #   JobName
     
    print('start_topics_detection_job_result: ', json.dumps(start_topics_detection_job_result))
     
    job_id = start_topics_detection_job_result["JobId"]
     
    print('job_id: ' + job_id)
    return job_id
    
def lambda_handler(event, context):
    comprehend = boto3.client(service_name='comprehend', region_name='ap-southeast-1')
    if "JobId" not in event:
        job_id = start_topic_modelling(comprehend)
    else:
        job_id = event["JobId"]
    
    describe_topics_detection_job_result = comprehend.describe_topics_detection_job(JobId=job_id)
    while describe_topics_detection_job_result['TopicsDetectionJobProperties']['JobStatus'] != 'COMPLETED' and \
        describe_topics_detection_job_result['TopicsDetectionJobProperties']['JobStatus'] != 'FAILED':
        describe_topics_detection_job_result = comprehend.describe_topics_detection_job(JobId=job_id)
     
    print('describe_topics_detection_job_result: ', describe_topics_detection_job_result)