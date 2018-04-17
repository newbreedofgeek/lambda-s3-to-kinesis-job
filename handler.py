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
    # here we get the file key that triggered this lambda
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

    try:
        # lets get that object now
        obj = s3.get_object(Bucket=bucket, Key=key)
        # read the file content as a stream
        bytestream = io.BytesIO(obj['Body'].read())
        # un-GZip it as text
        got_text = gzip.GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
        # create an array of lines from the file
        lines = got_text.splitlines()
        pubs = []

        for line in lines:
          toJson = json.loads(line)
          #identifier = 'Something unique to assist sharding (push updates of same items down the same shard)'

          publish = Publish(toJson, identifier)
          pubs.append(dict(publish))

        for pub in pubs:
            time.sleep(0.02) # a sleep is useful for throttling, or you should have many many kinesis shards (or both)
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

# A custom class we can use to wrap our kinesis messages
class Publish:
    def __init__(self, data, identifier):
        self.eventSource = 'Origin'
        self.eventId = str(uuid.uuid4())
        self.eventTime = datetime.datetime.now().isoformat()
        self.data = data
        self.eventMessage = 'Not needed'
        # self.partitionkey = identifier

    def __iter__(self):
        return self.__dict__.iteritems()
