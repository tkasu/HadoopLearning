#!/bin/bash

aws s3 cp ./input.nosync/ftp.ncdc.noaa.gov/pub/data/noaa/*  s3://hadoopbook-tomi/ncdc --recursive