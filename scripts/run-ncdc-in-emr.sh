#!/bin/bash

aws emr create-cluster --name "ncdc test cluster" --release-label emr-5.23.0 \
--use-default-roles --instance-type m4.large --instance-count 3 \
--steps Type=CUSTOM_JAR,Name="NCDC Jar Step",ActionOnFailure=CONTINUE,Jar=s3://hadoopbook-tomi/jars/hadoop-learning-0.1-shaded.jar,Args=["s3://hadoopbook-tomi/ncdc/","s3://hadoopbook-tomi/out/max-temp-3"] \
--log-uri "s3://hadoopbook-tomi/logs/" \
--auto-terminatel
