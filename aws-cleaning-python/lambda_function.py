import json
import io

from decoder import decode
from s3 import upload_fileobj
from cleaner import clean

BUCKET = "generate-insights"
# BUCKET = "hackathon-comprehend-raw-results"

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
        csv_str_cleaned = clean(csv_str_file)
        csv_str_buf = io.BytesIO(csv_str_cleaned.encode('utf-8'))

        success = upload_fileobj(csv_str_buf, BUCKET, filename)
        if success:
            uploaded.append(filename)

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