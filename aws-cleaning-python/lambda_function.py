import json
import io

from decoder import decode
from s3 import put_object
from cleaner import clean

BUCKET = "hackathon-cleaned-data"
# BUCKET = "generate-insights"

def lambda_handler(event, context):
    # retrieve all files as bytes
    files = decode(event)

    # clean all files
    uploaded = []
    for csvFile in files:
        filename = csvFile.filename
        bytes_str = csvFile.bytesStr
        csv_str = bytes_str.decode('utf-8-sig')
        csv_str_file = io.StringIO(csv_str)
        text_headerless, text_timestamp_header = clean(csv_str_file)

        csv_str_buf_text = io.BytesIO(text_headerless.encode('utf-8'))
        filename_text = "text-" + filename
        success = put_object(csv_str_buf_text, BUCKET, filename_text)
        if success:
            uploaded.append(filename_text)

        csv_str_buf_text_timestamp = io.BytesIO(text_timestamp_header.encode('utf-8'))
        filename_text_timestamp = "text-timestamp-" + filename
        success = put_object(csv_str_buf_text_timestamp, BUCKET, filename_text_timestamp)
        if success:
            uploaded.append(filename_text_timestamp)

    return {
        'statusCode': 200,
        'body': json.dumps({
            "uploaded": uploaded
        }),
    }

# with open('input/event.json') as file:
#     # extract the form-data
#     event = json.load(file)
#     print(event)
#     print(lambda_handler(event, None))