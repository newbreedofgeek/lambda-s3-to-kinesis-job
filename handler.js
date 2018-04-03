from __future__ import print_function

import urllib
import boto3
import io
import gzip
import json
import uuid
import datetime
import base64

s3 = boto3.client('s3')
kinesis_client = boto3.client('kinesis', region_name='YOUR-REGION-HERE')
bucket = 'YOUR-INPUT-BUCKET-HERE'
stream = 'YOUR-OUTPUT-STREAM-HERE'

def lambda_handler(event, context):
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        bytestream = io.BytesIO(obj['Body'].read())
        got_text = gzip.GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
        lines = got_text.splitlines()
        pubs = []

        for line in lines:
          // TODO implement

        for pub in pubs:
            put_to_stream(pub)

        print('%s items pushed to kinesis.'%(len(pubs)))

    except Exception as e:
      print(e)
      print('[CRITICAL] issues in workflow, error details are {}'.format(e))
      raise e

    return 'Done'


def put_to_stream(payload):
    put_response = kinesis_client.put_record(
                        StreamName=stream,
                        Data=json.dumps(payload),
                        PartitionKey=payload['partitionkey'])
