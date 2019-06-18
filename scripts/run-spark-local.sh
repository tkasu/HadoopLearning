#!/usr/bin/env bash

cd ..

# Params input and output
spark-submit --class ncdc_analysis.spark.temperature.MaxTemperatureApp \
--packages com.databricks:spark-csv_2.11:1.5.0  \
--master local[*] \
target/hadoop-learning-0.1-shaded.jar \
$1 $2
