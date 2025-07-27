import json

import boto3

def lambda_handler(event, context):
    try:
        message = event['Records'][0]['Sns']['Message']
        filename = json.loads(message)['Records'][0]['s3']['object']['key']
        client = boto3.client('glue')
        client.start_job_run(JobName='b3_visual_glue_job', Arguments={'--filename': filename})
        print('Calling glue job with filename', filename)
        return {
            'statusCode': 200,
            'body': json.dumps('Lambda trigger executed successfully!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps('Lambda trigger failed!')
        }
