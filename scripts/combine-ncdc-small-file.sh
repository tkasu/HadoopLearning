#!/bin/bash

cd ../src/main/python
python ncdc_analysis/preprocessing/combine_files_to_yearly.py \
    "../../../input.nosync/ftp.ncdc.noaa.gov/pub/data/noaa_testing/" \
    "../../../input.nosync/ncdc_processed/yearly/gz/testing"
