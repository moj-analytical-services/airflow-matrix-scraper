import pytest
import unittest
import json

from moto import mock_s3
import gzip
import boto3
from functions.api_requests import (
    unpack_row,
    unpack_data,
    split_s3_path,
    write_dicts_to_json,
)

input_dict = {
    "col1": "test-data",
    "col2": {"nest1name1": {"nest2": {"nest3": "val1123"}}, "nest1name2": "val12"},
    "col3": [{"name": "val1"}, {"name": "val2"}, {"name": "val3"}],
}
expected_dict = {
    "col1": "test-data",
    "col2.nest1name1.nest2.nest3": "val1123",
    "col2.nest1name2": "val12",
    "col3.1.name": "val1",
    "col3.2.name": "val2",
    "col3.3.name": "val3",
}


def test_unpack_row():
    actual = unpack_row(input_dict)
    assert expected_dict == actual


def test_unpack_data():
    input_data = [input_dict for _ in range(5)]
    expected = [expected_dict for _ in range(5)]
    actual = unpack_data(input_data)
    assert expected == actual


def test_split_s3_path():
    test_path = "s3://bucket-name/file/to/path.txt"
    expected = "bucket-name", "file/to/path.txt"
    actual = split_s3_path(test_path)
    assert expected == actual


def test_split_s3_path_error():
    test_path = "bucket-name/file/to/path.txt"
    with pytest.raises(ValueError):
        split_s3_path(test_path)


@mock_s3
def test_write_dicts_to_json():
    bucket = "test-bucket"
    key = "path/to/file.json.gz"
    data = [{"test": 1}, {"test": 2}]

    # Create a mock bucket
    s3 = boto3.client("s3", region_name="eu-west-2")
    location = {"LocationConstraint": "eu-west-2"}
    s3.create_bucket(Bucket=bucket, CreateBucketConfiguration=location)

    # Add compressed data to bucket
    write_dicts_to_json(data, f"s3://{bucket}/{key}")

    # Get object back
    object_from_s3 = s3.get_object(Bucket=bucket, Key=key)
    with gzip.GzipFile(fileobj=object_from_s3["Body"]) as gzipfile:
        actual_data = gzipfile.read()
    decoded_data = actual_data.decode("utf-8").split("\n")
    final_data = [json.loads(line) for line in decoded_data]

    assert data == final_data
