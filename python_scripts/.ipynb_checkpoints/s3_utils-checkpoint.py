import boto3
import botocore
import json

def read_json_from_s3(s3_path):
    bucket, key = s3_path_to_bucket_key(s3_path)
    obj = s3_resource.Object(bucket, key)
    text = obj.get()['Body'].read().decode('utf-8')
    return json.loads(text)

def s3_path_to_bucket_key(path):
    path = path.replace("s3://", "")
    bucket, key = path.split('/', 1)
    return bucket, key

def s3_object_exists(bucket, path):
    try:
        s3_resource.Object(bucket, path).load()
        return True
    except botocore.exceptions.ClientError as e:
        return False

s3_resource = boto3.resource('s3')

