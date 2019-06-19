import boto3
import pytest
from ncdc_analysis.aws.s3 import S3Path, s3_listdir, s3_read_to_mem
from moto import mock_s3


def test_S3Path_from_init():
    bucket = "example_bucket"
    key = "this/is/example/key"
    path_obj = S3Path(bucket, key)
    assert path_obj.path == "s3://example_bucket/this/is/example/key"
    assert path_obj.bucket == bucket
    assert path_obj.key == key


def test_S3Path_from_path():
    example_path = "s3://example_bucket/this/is/example/key"
    path_obj = S3Path.from_path(example_path)
    assert path_obj.path == example_path
    assert path_obj.bucket == "example_bucket"
    assert path_obj.key == "this/is/example/key"


def test_S3Path_join():
    example_path = "s3://example_bucket/this/is/example"
    path_obj = S3Path.from_path(example_path)
    new_obj = path_obj.join("key")
    assert path_obj.key == "this/is/example"
    assert isinstance(new_obj, S3Path)
    assert new_obj.key == "this/is/example/key"


@mock_s3
def test_s3_listdir():
    s3_res = boto3.resource("s3")
    bucket_name = "test-bucket"
    s3_res.create_bucket(Bucket=bucket_name)

    obj_1 = s3_res.Object(bucket_name, "my/ls/test/key_1")
    obj_2 = s3_res.Object(bucket_name, "my/ls/test/key_2")
    obj_1.put(Body=b"")
    obj_2.put(Body=b"")

    s3_client = boto3.client("s3")
    path_obj = S3Path(bucket=bucket_name, key="my/ls/test/")
    keys = s3_listdir(s3_client, path_obj)

    assert isinstance(keys, list)
    assert {d["Key"] for d in keys} == {"my/ls/test/key_1", "my/ls/test/key_2"}


@mock_s3
def test_s3_read_to_mem():
    s3_res = boto3.resource("s3")
    bucket_name = "test-bucket"
    s3_res.create_bucket(Bucket=bucket_name)

    obj_1 = s3_res.Object(bucket_name, "my/read_to_mem/test/key_1")
    obj_1.put(Body=b"Hello memory")

    s3_client = boto3.client("s3")
    path_obj = S3Path(bucket=bucket_name, key="my/read_to_mem/test/key_1")
    data = s3_read_to_mem(s3_client, path_obj)
    assert data == "Hello memory"


