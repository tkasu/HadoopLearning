import click
from ..core.cluster import run_mapr_job, run_spark_job
from settings import NCDC_S3_JAR_PATH, NCDC_S3_LOGS_PATH, NCDC_S3_OUT_PATH, LOCAL_OUTPUT_PATH, \
    NCDC_S3_DATA_PROD_PATH, NCDC_S3_DATA_TEST_PATH


@click.command()
@click.option("--job-type", help="EMR job type for, 'spark' or 'mapreduce'")
@click.option("--region", help="AWS Region to use, e.g. eu-central-1, efaults to env variable AWS_REGION")
@click.option("--jar-path", default=NCDC_S3_JAR_PATH,
              help="S3 path to .jar, defaults to env variable NCDC_JARS_S3_PATH")
@click.option("--logs-path", default=NCDC_S3_LOGS_PATH,
              help="S3 output path for logs, defaults to env variable NCDC_LOGS_S3_PATH")
@click.option("--input", default="test",
              help="""Input data used for the job. Accepts following parameters:
              1) test (default) => Uses env variable NCDC_LOGS_S3_DATA_TEST_PATH
              2) prod => Uses env variable NCDC_LOGS_S3_DATA_PROD_PATH
              3) other => Tries use input as S3 path for input data""")
@click.option("--out-s3", default=NCDC_S3_OUT_PATH,
              help="S3 path used to output results, defaults to env variable NCDC_S3_OUT_PATH")
@click.option("--out-local", default=LOCAL_OUTPUT_PATH,
              help="local path used to output results, defaults to env variable NCDC_S3_OUT_PATH")
@click.option("--instance-type", default="m4.large",
              help="EMR instance type, used for master and slave instances.")
@click.option("--instance-count", default=3,
              help="Number of instances used for the EMR cluster.")
def runner(job_type, jar_path, logs_path, input, out_s3, out_local, instance_type, instance_count):
    print(job_type)  # TODO


if __name__ == "__main__":
    runner()
