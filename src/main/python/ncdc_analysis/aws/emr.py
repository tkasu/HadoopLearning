from abc import abstractmethod, ABCMeta
import boto3
from dataclasses import dataclass, field
from itertools import chain
import math
from ncdc_analysis.aws.s3 import S3Path
from ncdc_analysis.postprocessing.result_fetchers import EMRResultFetcher
from settings import AWS_REGION
from typing import Any, Dict, Optional, List


@dataclass
class EMRStep(metaclass=ABCMeta):
    """Helper Abstract class to create steps for EMRConfigBuilder"""
    # required_emr_applications: List[str]

    @abstractmethod
    def to_dict(self) -> Dict:
        """Returned dictionary should be valid format for boto3's EMR-client in Steps-keyword"""
        pass


@dataclass
class EMRHadoopStep(EMRStep):
    jar_path: str
    jar_args: List[str]
    name: str = "Hadoop Jar Step"
    action_on_failure: str = "CONTINUE"

    # Class args
    required_emr_applications: List[str] = field(default_factory=list)

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
    jar_path: str
    jar_class: str
    jar_args: List[str]
    packages: List[str] = field(default_factory=list)
    name: str = "Spark Jar Step"
    action_on_failure: str = "CONTINUE"

    # Class args
    _args_defaults = ["spark-submit",
                      "--deploy-mode", "cluster",
                      "--master", "yarn"]
    required_emr_applications = ["Spark"]

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
        self._steps = []

    def add_step(self, step: EMRStep):
        self._steps.append(step)

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
        steps_app_reqs = [step.required_emr_applications for step in self._steps]
        app_reqs = list(set(chain.from_iterable(steps_app_reqs)))
        if not app_reqs:
            return {}
        app_reqs_dicts = [{"Name": req} for req in app_reqs]
        return dict(Applications=app_reqs_dicts)

    def to_dict(self) -> Dict:
        """Creates dictionary that can be passed to boto3.
        As boto3 uses keyword format instead of dictionary format, dictionary can be passed with keyword stars:
        e.g. **inst.to_dict"""
        if len(self._steps) == 0:
            raise ValueError("You need to add steps before using the config.")
        return {**self._build_config_base(), **self._build_config_applications(), **self._build_config_steps()}


class EMRRunner:
    config: EMRConfigBuilder
    result_fetcher: EMRResultFetcher
    output_path: S3Path
    results: Optional[Any] = None
    max_wait: int
    wait_for_completion: bool
    _client = None
    _cluster_id: Optional[str]

    def __init__(self, config, output_path: S3Path, result_fetcher=None, max_wait=900, wait_for_completion=True):
        self.config = config
        self.output_path = output_path
        self.result_fetcher = result_fetcher
        self.max_wait = max_wait
        self.wait_for_completion = wait_for_completion
        self._init_emr_session()

    def execute(self):
        self._send_job_to_emr()
        if not self.wait_for_completion:
            print("Job sent to EMR.")
            return
        self._wait_for_cluster_completion()
        if self.result_fetcher:
            self.fetch_results()
            print("Results fetched")

    def fetch_results(self):
        self.results = self.result_fetcher.fetch(self.output_path)

    def _send_job_to_emr(self):
        """Send job to AWS EMR. Returns cluster identification that can be used to request more details with boto3"""
        cluster: Dict = self._client.run_job_flow(**self.config.to_dict())
        cluster_id = cluster["JobFlowId"]
        self._cluster_id = cluster_id

    def _init_emr_session(self):
        session = boto3.Session(profile_name="emr_runner")
        client = session.client("emr", region_name=AWS_REGION)
        self._client = client

    def _wait_for_cluster_completion(self) -> None:
        """Blocks until all EMR Steps has been completed.
        Max wait is approximately equivalent to self.max_wait in seconds, rounded up to next 30s."""
        client = self._client
        cluster_id = self._cluster_id

        steps = client.list_steps(ClusterId=cluster_id)["Steps"]

        waiter = client.get_waiter("step_complete")
        print("Waiting until EMR job has been completed")
        delay = 30
        max_attempts = math.ceil(self.max_wait / delay)
        for step in steps:
            print(f"Waiting for EMR-step {step['Name']}")
            waiter.wait(
                ClusterId=cluster_id,
                StepId=step["Id"],
                WaiterConfig={
                    "Delay": delay,
                    "MaxAttempts": max_attempts
                }
            )
        print("EMR Job completed!")

