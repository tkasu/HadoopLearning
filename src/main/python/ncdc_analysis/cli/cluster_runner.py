import click
from ..core.cluster import run_mapr_job, run_spark_job
from settings import NCDC_S3_JAR_PATH, NCDC_S3_LOGS_PATH, NCDC_S3_OUT_PATH, LOCAL_OUTPUT_PATH, \
    NCDC_S3_DATA_PROD_PATH, NCDC_S3_DATA_TEST_PATH


@click.command()
@click.option("--job-type", help="EMR job type", type=click.Choice(["mapreduce", "spark"]))
@click.option("--jar-path", default=NCDC_S3_JAR_PATH,
              help="S3 path to .jar, defaults to env variable NCDC_JARS_S3_PATH")
@click.option("--jar-class", help="Class to run with Spark, not supported with job-type mapreduce")
@click.option("--packages",
              help="Extra packages provided for Spark (see. spark-submit), separate packages with commas ','. "
                   "Not supported with job-type mapreduce")
@click.option("--logs-path", default=NCDC_S3_LOGS_PATH,
              help="S3 output path for logs, defaults to env variable NCDC_LOGS_S3_PATH")
@click.option("--input-data", default="test",
              help="""Input data used for the job. Accepts following parameters:
              1) test (default) => Uses env variable NCDC_LOGS_S3_DATA_TEST_PATH
              2) prod => Uses env variable NCDC_LOGS_S3_DATA_PROD_PATH
              3) other => Tries use input_data as S3 path for input_data data""")
@click.option("--out-s3", default=NCDC_S3_OUT_PATH,
              help="S3 path used to output results, defaults to env variable NCDC_S3_OUT_PATH")
@click.option("--out-local", default=LOCAL_OUTPUT_PATH,
              help="local path used to output results, defaults to env variable NCDC_S3_OUT_PATH")
@click.option("--instance-type", default="m4.large",
              help="EMR instance type, used for master and slave instances.")
@click.option("--instance-count", default=3,
              help="Number of instances used for the EMR cluster.")
def runner(job_type, jar_path, jar_class, packages, logs_path, input_data, out_s3, out_local,
           instance_type, instance_count):
    if input_data == "prod":
        input_data = NCDC_S3_DATA_PROD_PATH
    elif input_data == "test":
        input_data = NCDC_S3_DATA_TEST_PATH

    if job_type == "mapreduce":
        if jar_class:
            raise ValueError("jar-class not supported with job-type mapreduce")
        if packages:
            raise ValueError("packages not supported with job-type mapreduce")
        run_mapr_job(input_path=input_data, jar_path=jar_path, logs_path=logs_path, out_s3=out_s3, out_local=out_local,
                     instance_count=instance_count, instance_type=instance_type)
    elif job_type == "spark":
        if not jar_class:
            raise ValueError("Please provide jar-class for spark job")
        if packages:
            packages = packages.split(",")
        run_spark_job(input_path=input_data, jar_path=jar_path, jar_class=jar_class, logs_path=logs_path,
                      out_s3=out_s3, out_local=out_local, packages=packages,
                      instance_count=instance_count, instance_type=instance_type)


if __name__ == "__main__":
    runner()
