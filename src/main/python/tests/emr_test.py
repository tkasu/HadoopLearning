import boto3
from ncdc_analysis.aws.emr import EMRConfigBuilder, send_job_to_emr
from moto import mock_emr, mock_s3
from typing import Dict
import pytest


@mock_emr
@pytest.fixture()
def emr_client():
    return boto3.client("emr")


@pytest.fixture()
def emr_mapr_config():
    config = EMRConfigBuilder(name="Test job",
                              instance_count=1,
                              instance_type="m4.large",
                              logs_path="s3://some-bucket/logs")
    config.add_hadoop_step(jar_path="s3://some-bukcet/jars/some.jar",
                           jar_args=["s3://some-bucket/data", "s3://some-bucket/results"])
    return config


@pytest.fixture()
def emr_spark_config():
    config = EMRConfigBuilder(name="Test job",
                              instance_count=1,
                              instance_type="m4.large",
                              logs_path="s3://some-bucket/logs")
    config.add_spark_step(jar_path="s3://some-bukcet/jars/some.jar",
                          jar_args=["s3://some-bucket/data", "s3://some-bucket/results"],
                          jar_class="some.fancy.spark.class")
    return config


class TestEMRConfig:
    default_name = "Test EMR Run"
    default_instance_count = 1
    default_instance_type = "m5.large"
    default_logs_path = "s3://some-bucket/logs"
    default_action_on_failure = "TERMINATE"

    @pytest.fixture()
    def emr_default_config(self):
        config = EMRConfigBuilder(name=self.default_name,
                                  instance_count=self.default_instance_count,
                                  instance_type=self.default_instance_type,
                                  logs_path=self.default_logs_path,
                                  action_on_failure=self.default_action_on_failure)
        return config

    def test_default_init_values(self, emr_default_config):
        assert emr_default_config.name == self.default_name
        assert emr_default_config.instance_count == self.default_instance_count
        assert emr_default_config.instance_type == self.default_instance_type
        assert emr_default_config.logs_path == self.default_logs_path
        assert emr_default_config.action_on_failure == self.default_action_on_failure

    def test_no_action_on_failure(self):
        config = EMRConfigBuilder(name=self.default_name,
                                  instance_count=self.default_instance_count,
                                  instance_type=self.default_instance_type,
                                  logs_path=self.default_logs_path)

        assert config.action_on_failure == "CONTINUE"

    def test_no_spark_by_default(self, emr_default_config):
        assert emr_default_config._spark == False

    def test_hadoop_step(self, emr_default_config):
        config = emr_default_config
        config.add_hadoop_step(name="My custom step",
                               jar_path="s3://my/jar/path",
                               jar_args=["s3://my/input", "s3://my/output"],
                               action_on_failure="TERMINATE")
        jar_step = config._steps[0]
        assert jar_step.name == "My custom step"
        assert jar_step.action_on_failure == "TERMINATE"
        assert jar_step.jar_path == "s3://my/jar/path"
        assert jar_step.jar_args == ["s3://my/input", "s3://my/output"]
        assert config._spark == False

    def test_hadoop_step_no_name(self, emr_default_config):
        config = emr_default_config
        config.add_hadoop_step(jar_path="s3://my/jar/path",
                               jar_args=["s3://my/input", "s3://my/output"],
                               action_on_failure="TERMINATE")
        jar_step = config._steps[0]
        assert jar_step.name == "Hadoop Jar Step"

    def test_hadoop_step_no_action_on_failure(self, emr_default_config):
        config = emr_default_config
        config.add_hadoop_step(jar_path="s3://my/jar/path",
                               jar_args=["s3://my/input", "s3://my/output"])
        jar_step = config._steps[0]
        assert jar_step.action_on_failure == "CONTINUE"

    def test_spark_step(self, emr_default_config):
        config = emr_default_config
        config.add_spark_step(name="My custom step",
                              jar_path="s3://my/jar/path",
                              jar_class="MySparkApp",
                              jar_args=["s3://my/input", "s3://my/output"],
                              packages=["com.databricks:spark-csv_2.11:1.5.0", "io.delta:delta-core_2.12:0.1.0"],
                              action_on_failure="TERMINATE")
        jar_step = config._steps[0]
        assert jar_step.name == "My custom step"
        assert jar_step.action_on_failure == "TERMINATE"
        assert jar_step.jar_path == "s3://my/jar/path"
        assert jar_step.jar_args == ["s3://my/input", "s3://my/output"]
        assert jar_step.jar_class == "MySparkApp"
        assert jar_step.packages == ["com.databricks:spark-csv_2.11:1.5.0", "io.delta:delta-core_2.12:0.1.0"]
        assert config._spark == True

    def test_to_dict_hadoop_step(self, emr_default_config):
        config = emr_default_config
        config.add_hadoop_step(name="My custom step",
                               jar_path="s3://my/jar/path",
                               jar_args=["s3://my/input", "s3://my/output"],
                               action_on_failure="TERMINATE")
        config_dict: Dict = config.to_dict()
        expected_dict = dict(
            Name=self.default_name,
            LogUri=self.default_logs_path,
            ReleaseLabel="emr-5.23.0",
            Instances={
                "MasterInstanceType": self.default_instance_type,
                "SlaveInstanceType": self.default_instance_type,
                "InstanceCount": self.default_instance_count,
            },
            VisibleToAllUsers=True,
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            Steps=[
                {
                    "Name": "My custom step",
                    "ActionOnFailure": "TERMINATE",
                    "HadoopJarStep": {
                        "Jar": "s3://my/jar/path",
                        "Args": ["s3://my/input", "s3://my/output"]
                    }
                }
            ]
        )
        assert config_dict == expected_dict

    def test_to_dict_spark_step(self, emr_default_config):
        config = emr_default_config
        config.add_spark_step(name="My custom step",
                              jar_path="s3://my/jar/path",
                              jar_class="MySparkApp",
                              jar_args=["s3://my/input", "s3://my/output"],
                              packages=["com.databricks:spark-csv_2.11:1.5.0", "io.delta:delta-core_2.12:0.1.0"],
                              action_on_failure="TERMINATE")
        config_dict: Dict = config.to_dict()
        expected_dict = dict(
            Name=self.default_name,
            LogUri=self.default_logs_path,
            ReleaseLabel="emr-5.23.0",
            Instances={
                "MasterInstanceType": self.default_instance_type,
                "SlaveInstanceType": self.default_instance_type,
                "InstanceCount": self.default_instance_count,
            },
            VisibleToAllUsers=True,
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            Applications=[{"Name": "Spark"}],
            Steps=[
                {
                    "Name": "My custom step",
                    "ActionOnFailure": "TERMINATE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["spark-submit",
                                 "--deploy-mode", "cluster",
                                 "--master", "yarn",
                                 "--class", "MySparkApp",
                                 "--packages", "com.databricks:spark-csv_2.11:1.5.0,io.delta:delta-core_2.12:0.1.0",
                                 "s3://my/jar/path",
                                 "s3://my/input", "s3://my/output"]
                    }
                }
            ]
        )
        assert config_dict == expected_dict

    def test_to_dict_no_steps_raises(self, emr_default_config):
        """We can't send jobs to EMR without any steps."""
        config = emr_default_config
        with pytest.raises(ValueError):
            _ = config.to_dict()

    def test_spark_to_dict_no_packages(self, emr_default_config):
        config = emr_default_config
        config.add_spark_step(name="My custom step",
                              jar_path="s3://my/jar/path",
                              jar_class="MySparkApp",
                              jar_args=["s3://my/input", "s3://my/output"],
                              action_on_failure="TERMINATE")
        config_dict: Dict = config.to_dict()
        expected_dict = dict(
            Name=self.default_name,
            LogUri=self.default_logs_path,
            ReleaseLabel="emr-5.23.0",
            Instances={
                "MasterInstanceType": self.default_instance_type,
                "SlaveInstanceType": self.default_instance_type,
                "InstanceCount": self.default_instance_count,
            },
            VisibleToAllUsers=True,
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            Applications=[{"Name": "Spark"}],
            Steps=[
                {
                    "Name": "My custom step",
                    "ActionOnFailure": "TERMINATE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["spark-submit",
                                 "--deploy-mode", "cluster",
                                 "--master", "yarn",
                                 "--class", "MySparkApp",
                                 "s3://my/jar/path",
                                 "s3://my/input", "s3://my/output"]
                    }
                }
            ]
        )
        assert config_dict == expected_dict


@mock_emr
def test_emr_mapr_job_send(emr_client, emr_mapr_config):
    cluster_id = send_job_to_emr(emr_client, emr_mapr_config)

    steps = emr_client.list_steps(ClusterId=cluster_id)["Steps"]
    statuses = [step["Status"]["State"] for step in steps]
    assert "STARTING" in statuses


@mock_emr
def test_emr_spark_job_send(emr_client, emr_spark_config):
    cluster_id = send_job_to_emr(emr_client, emr_spark_config)

    steps = emr_client.list_steps(ClusterId=cluster_id)["Steps"]
    statuses = [step["Status"]["State"] for step in steps]
    assert "STARTING" in statuses
