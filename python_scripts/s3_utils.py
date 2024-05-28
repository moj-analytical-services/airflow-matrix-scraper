import boto3
import botocore
from botocore.exceptions import NoCredentialsError, ClientError
import logging
import json
import os
from datetime import datetime, timedelta


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(funcName)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger("__name__")

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
        if resp.get("Contents", None) is not None:
            for obj in resp.get("Contents", None):
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
        s3_resource.Object(bucket, key).delete()


def generate_date_strings(start_date, end_date, fmt="%Y-%m-%d"):
    start = datetime.strptime(start_date, fmt)
    end = datetime.strptime(end_date, fmt)
    date_strings = []
    current_date = start

    while current_date <= end:
        date_strings.append(current_date.strftime(fmt))
        current_date += timedelta(days=1)

    return date_strings

def location_data_refresh(end_date: str, source, destination): 
    # See https://github.com/moj-analytical-services/airflow-matrix-scraper/issues/56#issuecomment-2129003183
    # Copying earliest historical location data from dev to prod 

    # Earliest correct location data
    start_date = "2024-03-25"
    fmt="%Y-%m-%d"
    bucket = "alpha-dag-matrix"
    base = "db"
    table = "locations"
    today = datetime.strftime(datetime.now(), f"{fmt}T%H-%m")

    source_root = os.path.join(base, source, table)
    dest_root = os.path.join(base, destination, table)
    archive_root = os.path.join(base, "archive", destination, table, today)

    date_list = generate_date_strings(start_date, end_date)
    for date in date_list:
        partition_file = f"scrape_date={date}/{date}.parquet"
        source_key = os.path.join(source_root, partition_file)
        archive_key = os.path.join(archive_root, partition_file)
        dest_key = os.path.join(dest_root, partition_file)
        
        try:
            backup_original = {
                'Bucket': bucket,
                'Key': dest_key
            }
            s3.copy(backup_original, bucket, archive_key)
            logger.info(f'File copied from {dest_key} to {archive_key} and then deleted')
            s3.delete_object(Bucket=bucket, Key=dest_key)
            
            copy_source  = {
                'Bucket': bucket,
                'Key': source_key
            }
            s3.copy(copy_source, bucket, dest_key)
            logger.info(f'File copied from {source_key}'
                        f' to {dest_key}-%H-%M')
        except NoCredentialsError:
            logger.error("AWS Credentials not available")
        except ClientError as e:
            logger.error(e)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 3:
        end_date = sys.argv[1]
        source = sys.argv[2]
        destination = sys.argv[3]
    else:
        print("Usage: python python_scripts/s3_utils.py <end_date> <source> <dest>")

    location_data_refresh(end_date, source, destination)