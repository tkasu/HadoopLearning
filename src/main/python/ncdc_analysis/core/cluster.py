from datetime import datetime
import os
from settings import NCDC_S3_OUT_PATH, NCDC_S3_JAR_PATH, NCDC_S3_LOGS_PATH, \
    NCDC_S3_DATA_PROD_PATH, NCDC_S3_DATA_TEST_PATH, LOCAL_OUTPUT_PATH
from ncdc_analysis.aws.s3 import S3Path
from ncdc_analysis.aws.emr import EMRRunner, EMRConfigBuilder, EMRSparkStep, EMRHadoopStep
from ncdc_analysis.postprocessing.result_fetchers import EMRResultCsvFetcher
from typing import Optional, List


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
    step = EMRHadoopStep(jar_path=jar_path, jar_args=[input_data_path, output_path])
    emr_config.add_step(step)

    csv_fetcher = EMRResultCsvFetcher()
    csv_fetcher.output_path = os.path.join(LOCAL_OUTPUT_PATH, f"{run_timestamp}_ncdc_emr_results.csv")
    csv_fetcher.col_names = val_col_names

    runner = EMRRunner(config=emr_config, output_path=S3Path.from_path(output_path),
                       result_fetcher=csv_fetcher)
    runner.execute()


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
    step = EMRSparkStep(jar_path=jar_path, jar_args=[input_data_path, output_path],
                        jar_class=jar_class,
                        packages=packages)
    emr_config.add_step(step)

    csv_fetcher = EMRResultCsvFetcher()
    csv_fetcher.output_path = os.path.join(LOCAL_OUTPUT_PATH, f"{run_timestamp}_ncdc_emr_results.csv")
    csv_fetcher.spark = True

    runner = EMRRunner(config=emr_config, output_path=S3Path.from_path(output_path),
                       result_fetcher=csv_fetcher)
    runner.execute()


if __name__ == "__main__":
    #run_mapr_job(1, "m4.large", val_col_names=["min", "max", "average", "count"])
    run_spark_job(5, "m5.xlarge",
                  jar_class="ncdc_analysis.spark.temperature.MaxTemperatureApp",
                  packages=["com.databricks:spark-csv_2.11:1.5.0"],
                  mode="prod")
