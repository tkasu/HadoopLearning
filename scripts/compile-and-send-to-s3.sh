#!/bin/bash

cd ..
mvn package
aws s3 cp target/hadoop-learning-0.1-shaded.jar $NCDC_JARS_S3_PATH
