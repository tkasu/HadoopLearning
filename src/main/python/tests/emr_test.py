import boto3
from dataclasses import dataclass
from ncdc_analysis.aws.emr import EMRConfigBuilder, EMRStep, EMRHadoopStep, EMRSparkStep, EMRRunner
from ncdc_analysis.aws.s3 import S3Path
from ncdc_analysis.postprocessing.result_fetchers import EMRResultCsvFetcher
from moto import mock_emr
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
    step = EMRHadoopStep(jar_path="s3://some-bukcet/jars/some.jar",
                         jar_args=["s3://some-bucket/data", "s3://some-bucket/results"])
    config.add_step(step)
    return config


@pytest.fixture()
def emr_spark_config():
    config = EMRConfigBuilder(name="Test job",
                              instance_count=1,
                              instance_type="m4.large",
                              logs_path="s3://some-bucket/logs")
    step = EMRSparkStep(jar_path="s3://some-bukcet/jars/some.jar",
                        jar_args=["s3://some-bucket/data", "s3://some-bucket/results"],
                        jar_class="some.fancy.spark.class")
    config.add_step(step)
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

    def test_hadoop_step(self, emr_default_config):
        config = emr_default_config
        step = EMRHadoopStep(name="My custom step",
                             jar_path="s3://my/jar/path",
                             jar_args=["s3://my/input", "s3://my/output"],
                             action_on_failure="TERMINATE")
        config.add_step(step)
        jar_step = config._steps[0]
        assert jar_step.name == "My custom step"
        assert jar_step.action_on_failure == "TERMINATE"
        assert jar_step.jar_path == "s3://my/jar/path"
        assert jar_step.jar_args == ["s3://my/input", "s3://my/output"]

    def test_hadoop_step_no_name(self, emr_default_config):
        config = emr_default_config
        step = EMRHadoopStep(jar_path="s3://my/jar/path",
                             jar_args=["s3://my/input", "s3://my/output"],
                             action_on_failure="TERMINATE")
        config.add_step(step)
        jar_step = config._steps[0]
        assert jar_step.name == "Hadoop Jar Step"

    def test_hadoop_step_no_action_on_failure(self, emr_default_config):
        config = emr_default_config
        step = EMRHadoopStep(jar_path="s3://my/jar/path",
                             jar_args=["s3://my/input", "s3://my/output"])
        config.add_step(step)
        jar_step = config._steps[0]
        assert jar_step.action_on_failure == "CONTINUE"

    def test_spark_step(self, emr_default_config):
        config = emr_default_config
        step = EMRSparkStep(name="My custom step",
                            jar_path="s3://my/jar/path",
                            jar_class="MySparkApp",
                            jar_args=["s3://my/input", "s3://my/output"],
                            packages=["com.databricks:spark-csv_2.11:1.5.0", "io.delta:delta-core_2.12:0.1.0"],
                            action_on_failure="TERMINATE")
        config.add_step(step)
        jar_step = config._steps[0]
        assert jar_step.name == "My custom step"
        assert jar_step.action_on_failure == "TERMINATE"
        assert jar_step.jar_path == "s3://my/jar/path"
        assert jar_step.jar_args == ["s3://my/input", "s3://my/output"]
        assert jar_step.jar_class == "MySparkApp"
        assert jar_step.packages == ["com.databricks:spark-csv_2.11:1.5.0", "io.delta:delta-core_2.12:0.1.0"]

    def test_to_dict_hadoop_step(self, emr_default_config):
        config = emr_default_config
        step = EMRHadoopStep(name="My custom step",
                             jar_path="s3://my/jar/path",
                             jar_args=["s3://my/input", "s3://my/output"],
                             action_on_failure="TERMINATE")
        config.add_step(step)
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
        step = EMRSparkStep(name="My custom step",
                            jar_path="s3://my/jar/path",
                            jar_class="MySparkApp",
                            jar_args=["s3://my/input", "s3://my/output"],
                            packages=["com.databricks:spark-csv_2.11:1.5.0", "io.delta:delta-core_2.12:0.1.0"],
                            action_on_failure="TERMINATE")
        config.add_step(step)
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
        step = EMRSparkStep(name="My custom step",
                            jar_path="s3://my/jar/path",
                            jar_class="MySparkApp",
                            jar_args=["s3://my/input", "s3://my/output"],
                            action_on_failure="TERMINATE")
        config.add_step(step)
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

    def test_multiple_app_reqs(self, emr_default_config):
        config = emr_default_config

        @dataclass
        class DummyHiveStep(EMRStep):
            required_emr_applications = ["Hadoop", "Hive"]

            def to_dict(self):
                return {"Name": "DummyHiveStep"}

        @dataclass
        class DummyPigStep(EMRStep):
            required_emr_applications = ["Pig", "Hadoop"]

            def to_dict(self):
                return {"Name": "DummyPigStep"}

        dummy_hive_step = DummyHiveStep()
        dummy_pig_step = DummyPigStep()

        config.add_step(dummy_hive_step)
        config.add_step(dummy_pig_step)

        config_dict: Dict = config.to_dict()
        apps = {app_dict["Name"] for app_dict in config_dict["Applications"]}
        assert apps == {"Hadoop", "Hive", "Pig"}


class TestEMRRunner:

    @pytest.fixture()
    def emr_test_config(self):
        config = EMRConfigBuilder(name="Test EMR Run",
                                  instance_count=1,
                                  instance_type="m5.large",
                                  logs_path="s3://some-bucket/logs",
                                  action_on_failure="TERMINATE")
        step = EMRHadoopStep(jar_path="s3://my/jar/path",
                             jar_args=["s3://my/input", "s3://my/output"],
                             action_on_failure="TERMINATE")
        config.add_step(step)
        return config

    def test_init(self, emr_test_config: EMRConfigBuilder):
        output_path = S3Path.from_path("s3://my/output")
        runner = EMRRunner(config=emr_test_config, output_path=output_path, max_wait=3600)
        assert runner.config == emr_test_config
        assert runner.max_wait == 3600
        assert runner.wait_for_completion
        assert runner.output_path == output_path

    def test_init_no_wait(self, emr_test_config: EMRConfigBuilder):
        output_path = S3Path.from_path("s3://my/output")
        runner = EMRRunner(config=emr_test_config, output_path=output_path, max_wait=3600, wait_for_completion=False)
        assert not runner.wait_for_completion

    def test_default_wait(self, emr_test_config: EMRConfigBuilder):
        output_path = S3Path.from_path("s3://my/output")
        runner = EMRRunner(config=emr_test_config, output_path=output_path)
        assert runner.max_wait == 900

    def test_value_fetcher(self, emr_test_config: EMRConfigBuilder):
        output_path = S3Path.from_path("s3://my/output")
        runner = EMRRunner(config=emr_test_config, output_path=output_path, result_fetcher=EMRResultCsvFetcher)
        assert runner.result_fetcher == EMRResultCsvFetcher
