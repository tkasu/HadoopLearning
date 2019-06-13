import pytest
from ncdc_analysis.aws.s3 import s3_path_to_folder_and_key


def test_s3_path_to_folder_and_key():
    example_path = "s3://example_bucket/this/is/example//key"
    bucket, key = s3_path_to_folder_and_key(example_path)
    assert bucket == "example_bucket"
    assert key == "this/is/example//key"
