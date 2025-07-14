import json
import os
import boto3
import requests
from datetime import datetime

s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get('BUCKET_NAME')

def lambda_handler(event, context):
    try:
        response = requests.get("https://bored.api.lewagon.com/api/activity")
        response.raise_for_status()
        data = response.json()
        activity_key = data.get('key', 'unknown_key')

        now = datetime.now()
        filename = f"raw_activity_{now.strftime('%Y%m%d_%H%M%S')}_{activity_key}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"raw-data/{filename}",
            Body=json.dumps(data)
        )
        return {'statusCode': 200, 'body': json.dumps('Success!')}
    except Exception as e:
        print(f"Error: {e}")
        raise e
