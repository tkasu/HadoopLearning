import boto3
import botocore.exceptions as boto_exception
from ncdc_analysis.aws.emr import create_emr_config, send_job_to_emr
from moto import mock_emr, mock_s3
import pytest


@mock_emr
@pytest.fixture()
def emr_client():
    return boto3.client("emr")


@pytest.fixture()
def emr_config():
    config = create_emr_config(1, "m4.large", "s3://some-bucket/logs", "s3://some-bukcet/jars/some.jar",
                               "s3://some-bucket/data", "s3://some-bucket/results")
    return config


@mock_emr
def test_emr_job_send(emr_client, emr_config):
    cluster_id = send_job_to_emr(emr_client, emr_config)

    steps = emr_client.list_steps(ClusterId=cluster_id)["Steps"]
    statuses = [step["Status"]["State"] for step in steps]
    assert "STARTING" in statuses


@mock_emr
@pytest.mark.xfail(reason="It seems that instance type check is not implemented in moto's mock_emr")
def test_emr_incorrect_instatance_type_raises(emr_client):

    incor_config = create_emr_config(1, "XL5.XXXL", "s3://some-bucket/logs", "s3://some-bukcet/jars/some.jar",
                                     "s3://some-bucket/data", "s3://some-bucket/results")
    with pytest.raises(boto_exception.ClientError):
        _ = send_job_to_emr(emr_client, incor_config)


