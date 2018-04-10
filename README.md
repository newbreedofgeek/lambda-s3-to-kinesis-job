# Lambda S3 to Kinesis Job
A Python AWS Lambda project that reads GZIP'ed files, unpacks them, reads JSON and streams items to Kinesis

### Notes
- The files being read are basically txt files, each row (separated by EOF) is a JSON blob
- Example raw file `data.json` and the GZiped version `data.json.gzip` is found in the mock dir

### Running it
- You need to create a new AWS Lambda and copy the `handler/py` code to it
- I recommend using the serverless framework as the deployment solution
