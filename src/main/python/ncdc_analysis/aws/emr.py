from abc import abstractmethod, ABCMeta
import boto3
from dataclasses import dataclass
from datetime import datetime
import os
from ncdc_analysis.aws.s3 import fetch_hadoop_style_results, S3Path
import pandas as pd
from settings import AWS_REGION, NCDC_S3_OUT_PATH, NCDC_S3_JAR_PATH, NCDC_S3_LOGS_PATH, \
    NCDC_S3_DATA_PROD_PATH, NCDC_S3_DATA_TEST_PATH, LOCAL_OUTPUT_PATH
from typing import Dict, Optional, List


@dataclass
class EMRStep(metaclass=ABCMeta):
    """Helper Abstract class to create steps for EMRConfigBuilder"""
    pass

    @abstractmethod
    def to_dict(self) -> Dict:
        """Returned dictionary should be valid format for boto3's EMR-client in Steps-keyword"""
        pass


@dataclass
class EMRHadoopStep(EMRStep):
    action_on_failure: str
    jar_path: str
    jar_args: List[str]
    name: str

    def to_dict(self) -> Dict:
        d = {
            "Name": self.name,
            "ActionOnFailure": self.action_on_failure,
            "HadoopJarStep": {
                "Jar": self.jar_path,
                "Args": self.jar_args
            }
        }
        return d


@dataclass
class EMRSparkStep(EMRStep):
    action_on_failure: str
    jar_path: str
    jar_class: str
    jar_args: List[str]
    packages: List[str]
    name: str
    _args_defaults = ["spark-submit",
                      "--deploy-mode", "cluster",
                      "--master", "yarn"]

    def _build_packages(self) -> List:
        return ["--packages", ",".join(self.packages)] if self.packages else []

    def to_dict(self) -> Dict:
        d = {
            "Name": self.name,
            "ActionOnFailure": self.action_on_failure,
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [*self._args_defaults,
                         *["--class", self.jar_class],
                         *self._build_packages(),
                         *[self.jar_path],
                         *self.jar_args]
            }
        }
        return d


class EMRConfigBuilder:
    """Builds boto3's EMR-client run configuration based on given parameters."""
    name: str
    instance_count: int
    instance_type: str
    logs_path: str
    action_on_failure: str
    _spark: bool
    _steps: List[EMRStep]

    def __init__(self, name, instance_count, instance_type, logs_path, action_on_failure="CONTINUE"):
        self.name = name
        self.instance_count = instance_count
        self.instance_type = instance_type
        self.logs_path = logs_path
        self.action_on_failure = action_on_failure
        self._spark = False
        self._steps = []

    def add_hadoop_step(self,
                        jar_path: str,
                        jar_args: List[str],
                        action_on_failure: Optional[str] = "CONTINUE",
                        name: Optional[str] = "Hadoop Jar Step"):
        """Adds basic Hadoop JAR-step, tested with MapReduce jobs."""
        step = EMRHadoopStep(name=name, action_on_failure=action_on_failure,
                             jar_path=jar_path, jar_args=jar_args)
        self._steps.append(step)

    def add_spark_step(self,
                       jar_path: str,
                       jar_args: List[str],
                       jar_class: str,
                       packages: Optional[List[str]] = None,
                       action_on_failure: Optional[str] = "CONTINUE",
                       name: Optional[str] = "Spark Jar Step"):
        """Adds Spark-step.
        Also signals for EMRConfigBuilder that Spark should be added as a Application-keyword to the config."""
        if not packages:
            packages = []
        step = EMRSparkStep(name=name, jar_path=jar_path, jar_args=jar_args, jar_class=jar_class,
                            packages=packages, action_on_failure=action_on_failure)
        self._steps.append(step)
        self._spark = True

    def _build_config_base(self) -> Dict:
        d = dict(
                Name=self.name,
                LogUri=self.logs_path,
                ReleaseLabel="emr-5.23.0",
                Instances={
                    "MasterInstanceType": self.instance_type,
                    "SlaveInstanceType": self.instance_type,
                    "InstanceCount": self.instance_count
                },
                VisibleToAllUsers=True,
                JobFlowRole="EMR_EC2_DefaultRole",
                ServiceRole="EMR_DefaultRole",
            )
        return d

    def _build_config_steps(self) -> Dict:
        steps: List[Dict] = [step.to_dict() for step in self._steps]
        return dict(Steps=steps)

    def _build_config_applications(self) -> Dict:
        return dict(Applications=[{"Name": "Spark"}]) if self._spark else {}

    def to_dict(self) -> Dict:
        """Creates dictionary that can be passed to boto3.
        As boto3 uses keyword format instead of dictionary format, dictionary can be passed with keyword stars:
        e.g. **inst.to_dict"""
        if len(self._steps) == 0:
            raise ValueError("You need to add steps before using the config.")
        return {**self._build_config_base(), **self._build_config_applications(), **self._build_config_steps()}


def wait_for_cluster_completion(client, cluster_id: str) -> None:
    """Blocks until all EMR Steps has been completed.
    Cheks results every 30 seconds, fails after 60 tries."""
    steps = client.list_steps(ClusterId=cluster_id)["Steps"]

    waiter = client.get_waiter("step_complete")
    print("Waiting until EMR job has been completed")
    for step in steps:
        print(f"Waiting for EMR-step {step['Name']}")
        waiter.wait(
            ClusterId=cluster_id,
            StepId=step["Id"],
            WaiterConfig={
                "Delay": 30,
                "MaxAttempts": 60
            }
        )
    print("EMR Job completed!")


def send_job_to_emr(emr_client, emr_config: EMRConfigBuilder) -> str:
    """Send job to AWS EMR. Returns cluster identification that can be used to request more details with boto3"""
    cluster: Dict = emr_client.run_job_flow(**emr_config.to_dict())
    cluster_id = cluster["JobFlowId"]
    return cluster_id


def run_mapr_job(instance_count: int, instance_type: str, mode: Optional[str] = None, val_col_names: Optional[List[str]] = None):
    """Run MapReduce job in EMR.
    Waits until the cluster has been terminated and saves the results to LOCAL_OUTPUT_PATH in .csv"""
    if not mode or mode == "test":
        input_data_path = NCDC_S3_DATA_TEST_PATH
    elif mode == "prod":
        input_data_path = NCDC_S3_DATA_PROD_PATH
    else:
        raise ValueError("Invalid run mode, valid values are 'test' and 'prod")

    run_timestamp: str = datetime.now().isoformat()
    output_path = os.path.join(NCDC_S3_OUT_PATH, run_timestamp)
    jar_path = os.path.join(NCDC_S3_JAR_PATH, "hadoop-learning-0.1-shaded.jar")

    emr_config = EMRConfigBuilder(name="MapReduce Job",
                                  instance_count=instance_count,
                                  instance_type=instance_type,
                                  logs_path=NCDC_S3_LOGS_PATH)
    emr_config.add_hadoop_step(jar_path=jar_path, jar_args=[input_data_path, output_path])

    # Credentials from /.aws/credentials
    # TODO make a specific profile for this application
    session = boto3.Session(profile_name="default")
    client = session.client("emr", region_name=AWS_REGION)
    cluster_id = send_job_to_emr(client, emr_config)

    wait_for_cluster_completion(client, cluster_id)
    result_df: pd.DataFrame = fetch_hadoop_style_results(S3Path.from_path(output_path), col_names=val_col_names)
    result_df.to_csv(os.path.join(LOCAL_OUTPUT_PATH, f"{run_timestamp}_ncdc_emr_results.csv"))


def run_spark_job(instance_count: int, instance_type: str, jar_class: str, packages: Optional[List[str]], mode: Optional[str] = None):
    """Run Spark job in EMR."""
    if not mode or mode == "test":
        input_data_path = NCDC_S3_DATA_TEST_PATH
    elif mode == "prod":
        input_data_path = NCDC_S3_DATA_PROD_PATH
    else:
        raise ValueError("Invalid run mode, valid values are 'test' and 'prod")

    run_timestamp: str = datetime.now().isoformat()
    output_path = os.path.join(NCDC_S3_OUT_PATH, run_timestamp)
    jar_path = os.path.join(NCDC_S3_JAR_PATH, "hadoop-learning-0.1-shaded.jar")

    emr_config = EMRConfigBuilder(name="Spark Job",
                                  instance_count=instance_count,
                                  instance_type=instance_type,
                                  logs_path=NCDC_S3_LOGS_PATH)
    emr_config.add_spark_step(jar_path=jar_path, jar_args=[input_data_path, output_path],
                              jar_class=jar_class,
                              packages=packages)

    session = boto3.Session(profile_name="default")
    client = session.client("emr", region_name=AWS_REGION)
    cluster_id = send_job_to_emr(client, emr_config)

    wait_for_cluster_completion(client, cluster_id)
    result_df: pd.DataFrame = fetch_hadoop_style_results(S3Path.from_path(output_path),
                                                         spark=True, col_names=True)
    result_df.to_csv(os.path.join(LOCAL_OUTPUT_PATH, f"{run_timestamp}_ncdc_emr_results.csv"))


if __name__ == "__main__":
    #run_mapr_job(1, "m4.large", val_col_names=["min", "max", "average", "count"])
    run_spark_job(5, "m5.xlarge",
                  jar_class="ncdc_analysis.spark.temperature.MaxTemperatureApp",
                  packages=["com.databricks:spark-csv_2.11:1.5.0"],
                  mode="prod")
