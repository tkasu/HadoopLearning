#!/bin/bash

rm -r ../input.nosync/*
wget -r ftp://ftp.ncdc.noaa.gov/pub/data/noaa/ -P ../input.nosync/
# Clean non-folders
rm ../input.nosync/ftp.ncdc.noaa.gov/pub/data/noaa/*.*