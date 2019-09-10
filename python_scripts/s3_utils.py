import boto3
import botocore
import json


s3_resource = boto3.resource("s3")
s3 = boto3.client("s3")


def read_json_from_s3(s3_path):
    bucket, key = s3_path_to_bucket_key(s3_path)
    obj = s3_resource.Object(bucket, key)
    text = obj.get()["Body"].read().decode("utf-8")
    return json.loads(text)


def s3_path_to_bucket_key(path):
    path = path.replace("s3://", "")
    bucket, key = path.split("/", 1)
    return bucket, key


def s3_object_exists(bucket, path):
    try:
        s3_resource.Object(bucket, path).load()
        return True
    except botocore.exceptions.ClientError:
        return False


def get_matching_s3_keys(bucket, prefix="", suffix=""):

    """
    Generate the keys in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    s3 = boto3.client("s3")
    kwargs = {"Bucket": bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs["Prefix"] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp["Contents"]:
            key = obj["Key"]
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break


def delete_all_matching_s3_objects(bucket, prefix="", suffix=""):
    for key in get_matching_s3_keys(bucket, prefix, suffix):
        s3.Object(bucket, key).delete()
