#!/bin/bash

hadoop fs -copyFromLocal ../input.nosync/ncdc_processed/yearly/gz/all/* hadoop-book/input/ncdc/yearly/gz/all/
