from datetime import datetime
import os
from ncdc_analysis.aws.s3 import S3Path
from ncdc_analysis.aws.emr import EMRRunner, EMRConfigBuilder, EMRSparkStep, EMRHadoopStep
from ncdc_analysis.postprocessing.result_fetchers import EMRResultCsvFetcher
from typing import Optional, List


def run_mapr_job(input_path: str,
                 jar_path: str,
                 logs_path: str,
                 out_s3: str,
                 out_local: str,
                 instance_count: int,
                 instance_type: str,
                 val_col_names: Optional[List[str]] = None):
    """Run MapReduce job in EMR.
    Waits until the cluster has been terminated and saves the results to LOCAL_OUTPUT_PATH in .csv"""

    run_timestamp: str = datetime.now().isoformat()
    output_path = os.path.join(out_s3, run_timestamp)

    emr_config = EMRConfigBuilder(name="MapReduce Job",
                                  instance_count=instance_count,
                                  instance_type=instance_type,
                                  logs_path=logs_path)
    step = EMRHadoopStep(jar_path=jar_path, jar_args=[input_path, output_path])
    emr_config.add_step(step)

    csv_fetcher = EMRResultCsvFetcher()
    csv_fetcher.output_path = os.path.join(out_local, f"{run_timestamp}_ncdc_emr_results.csv")
    csv_fetcher.col_names = val_col_names

    runner = EMRRunner(config=emr_config, output_path=S3Path.from_path(output_path),
                       result_fetcher=csv_fetcher)
    runner.execute()


def run_spark_job(input_path: str,
                  jar_path: str,
                  logs_path: str,
                  out_s3: str,
                  out_local: str,
                  instance_count: int,
                  instance_type: str,
                  jar_class: str,
                  packages: Optional[List[str]]):
    """Run Spark job in EMR."""
    run_timestamp: str = datetime.now().isoformat()
    output_path = os.path.join(out_s3, run_timestamp)

    emr_config = EMRConfigBuilder(name="Spark Job",
                                  instance_count=instance_count,
                                  instance_type=instance_type,
                                  logs_path=logs_path)
    step = EMRSparkStep(jar_path=jar_path, jar_args=[input_path, output_path],
                        jar_class=jar_class,
                        packages=packages)
    emr_config.add_step(step)

    csv_fetcher = EMRResultCsvFetcher()
    csv_fetcher.output_path = os.path.join(out_local, f"{run_timestamp}_ncdc_emr_results.csv")
    csv_fetcher.spark = True

    runner = EMRRunner(config=emr_config, output_path=S3Path.from_path(output_path),
                       result_fetcher=csv_fetcher)
    runner.execute()
