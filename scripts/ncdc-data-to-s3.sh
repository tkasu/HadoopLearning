#!/bin/bash

aws s3 cp ../input.nosync/ncdc_processed/yearly/gz/all $NCDC_LOGS_S3_DATA_PROD_PATH --recursive