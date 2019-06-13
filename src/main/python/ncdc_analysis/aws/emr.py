import boto3
from datetime import datetime
import os
from ncdc_analysis.aws.s3 import fetch_mapreduce_results
import pandas as pd
from settings import AWS_REGION, NCDC_LOGS_S3_OUT_PATH, NCDC_S3_JAR_PATH, NCDC_S3_LOGS_PATH, \
    NCDC_S3_DATA_PROD_PATH, NCDC_S3_DATA_TEST_PATH, LOCAL_OUTPUT_PATH
from typing import Dict, Optional, List


def wait_for_cluster_completion(client, cluster_id: int) -> None:
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


def run_job(instance_count: int, instance_type: str, mode: Optional[str] = None, val_col_names: Optional[List[str]] = None):
    """Run MapReduce job in EMR.
    Waits until the cluster has been terminated and saves the results to LOCAL_OUTPUT_PATH in .csv"""
    if not mode or mode == "test":
        data_bucket = NCDC_S3_DATA_TEST_PATH
    elif mode == "prod":
        data_bucket = NCDC_S3_DATA_PROD_PATH
    else:
        raise ValueError("Invalid run mode, valid values are 'test' and 'prod")

    run_timestamp: str = datetime.now().isoformat()
    output_bucket = os.path.join(NCDC_LOGS_S3_OUT_PATH, run_timestamp)

    # Credentials from /.aws/credentials
    # TODO make a specific profile for this application
    session = boto3.Session(profile_name="default")

    client = session.client(
        "emr",
        region_name=AWS_REGION
    )

    cluster: Dict = client.run_job_flow(
        Name="emr_mapreduce_test",
        LogUri=NCDC_S3_LOGS_PATH,
        ReleaseLabel="emr-5.23.0",
        Instances={
            "MasterInstanceType": instance_type,  # e.g. "m4.large"
            "SlaveInstanceType": instance_type,  # e.g. "m4.large"
            "InstanceCount": instance_count,
        },
        Steps=[
            {
                "Name": "NCDC Jar Step",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": os.path.join(NCDC_S3_JAR_PATH, "hadoop-learning-0.1-shaded.jar"),
                    "Args": [data_bucket, output_bucket]
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        # Setting below do not work, but it seems that AutoTerminatino is on by default. How can this be guaranteed?
        #AutoTerminate=True)
    )

    cluster_id = cluster["JobFlowId"]
    wait_for_cluster_completion(client, cluster_id)
    result_df: pd.DataFrame = fetch_mapreduce_results(output_bucket, val_col_names=val_col_names)
    result_df.to_csv(os.path.join(LOCAL_OUTPUT_PATH, f"{run_timestamp}_ncdc_emr_results.csv"))


if __name__ == "__main__":
    run_job(1, "m4.large", val_col_names=["min", "max", "average", "count"])