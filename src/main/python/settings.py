from dotenv import load_dotenv, find_dotenv
import os
from pathlib import Path

load_dotenv(find_dotenv())

AWS_REGION = os.getenv("AWS_REGION")
NCDC_S3_JAR_PATH = os.getenv("NCDC_JARS_S3_PATH")
NCDC_S3_LOGS_PATH = os.getenv("NCDC_LOGS_S3_PATH")
NCDC_S3_DATA_TEST_PATH = os.getenv("NCDC_LOGS_S3_DATA_TEST_PATH")
NCDC_S3_DATA_PROD_PATH = os.getenv("NCDC_LOGS_S3_DATA_PROD_PATH")
NCDC_LOGS_S3_OUT_PATH = os.getenv("NCDC_LOGS_S3_OUT_PATH")
LOCAL_OUTPUT_PATH = os.getenv("LOCAL_OUTPUT_PATH")
