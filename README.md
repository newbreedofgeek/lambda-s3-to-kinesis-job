# Lambda S3 to Kinesis Job
A Python AWS gitLambda project that reads GZIP'ed files, unpacks them, reads JSON and streams items to Kinesis


### Notes
- The files being read are basically txt files, each row (separated by EOF) is a JSON blob
