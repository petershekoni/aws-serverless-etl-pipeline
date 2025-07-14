import json
import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

s3 = boto3.client('s3')
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = os.environ.get('BUCKET_NAME')
TABLE_NAME = os.environ.get('TABLE_NAME')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

def lambda_handler(event, context):
    try:
        key = None 
        s3_event = event['Records'][0]['s3']
        bucket = s3_event['bucket']['name']
        key = s3_event['object']['key']

        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))

        if not all(k in data for k in ('key', 'activity')) or not data['key'] or not data['activity']:
            failure_message = f"Data quality check failed for file {key}. Missing 'key' or 'activity'."
            print(failure_message)
            sns.publish(TopicArn=SNS_TOPIC_ARN, Message=failure_message, Subject="Data Quality Alert")
            return {'statusCode': 400, 'body': json.dumps(failure_message)}

        df = pd.json_normalize(data)

        # --- FIXES ---
        df['price'] = df['price'].astype(float)
        df['accessibility'] = df['accessibility'].astype(float)
        # ---------------

        parquet_key = key.replace('raw-data/', 'processed-data/').replace('.json', '.parquet')
        buffer = pa.BufferOutputStream()
        pq.write_table(pa.Table.from_pandas(df), buffer)
        s3.put_object(Bucket=BUCKET_NAME, Key=parquet_key, Body=buffer.getvalue().to_pybytes())

        table = dynamodb.Table(TABLE_NAME)
        item = {
            'activity_key': str(df['key'].iloc[0]),
            'activity': str(df['activity'].iloc[0]),
            'type': str(df['type'].iloc[0]),
            'participants': int(df['participants'].iloc[0]),
            'price': str(df['price'].iloc[0]),
        }
        table.put_item(Item=item)

        return {'statusCode': 200, 'body': json.dumps('Successfully processed data.')}

    except Exception as e:
        error_message = f"CRITICAL: Lambda processing failed. Error: {str(e)}"
        if key:
             error_message = f"CRITICAL: Lambda processing failed for file {key}. Error: {str(e)}"

        print(error_message)
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=error_message,
            Subject="Lambda Processing Failure"
        )
        raise e