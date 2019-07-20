# HadoopLearning

My journey to learn (and in some cases refresh) following tools and platforms:

* Hadoop
* AWS 
* Spark
* Beyond Java 6
* And there will be more ;)

At the beginning this was a project template to test examples in the [Hadoop Book](http://hadoopbook.com/), but this has evolved as a toy project to try different tools.

## Disclaimer

This project is a set of small programs and scripts that I have used to learn the concepts. 
Even though I try to document what I have done, the code and scripts are not build like production grade software.

Also if using Python EMR runner, please check that the EC2 instance will terminate (even though in my case that always happened).

## Requirements

* Java 1.8
* Maven
* Python Anaconda Distribution
* Spark (to run Spark locally, can be run in cluster without this)
* Hadoop (to run MapReduce locally, can be run in cluster without this)

## Installation 

### Environment variables

#### Required

* AWS_REGION = aws region to use, e.g. eu-central-1

#### Optional (but recommended)

* NCDC_JARS_S3_PATH = S3 Path to .jar to run in EMR. See Java installation how to obtain it.
* NCDC_LOGS_S3_PATH = S3 Path to use for EMR logging 
* NCDC_LOGS_S3_DATA_TEST_PATH = S3 Path for 
* NCDC_S3_DATA_PROD_PATH = Path to S3 folder/prefix where test input data is located
* NCDC_S3_OUT_PATH = Path to S3 folder/prefix where test produciton data is located
* LOCAL_OUTPUT_PATH = Local path where final results are fetched 

#### .env

By default, python scripts try to load enviroment variables from .env file from to project root, see .env.template.

### Java

#### Building the project

This project uses maven and can be packaged with:

```bash
$ mvn package
```

Note that if you want to use to Python EMR cluster_runner, you have to manually create S3 bucket and upload the .jar, see e.g. scripts/compile-and-send-to-s3.sh

### Python

This project uses conda, so you need at least miniconda installation.


Needed libraries can be found from py_environment.yml and can be installed and activated in following way:

```bash
# Modify prefix in py_environemnet.yml to match you anaconda installation and
$ conda env create -f py_environment.yml
$ conda activate hadoop-learning
```

### Hadoop

To run examples locally, install Hadoop 2.x.x. https://hadoop.apache.org/releases.html

To install and configure Hadoop, [Hadoop Book](http://hadoopbook.com/) is a good resource.

### Spark

To run Spark examples locally, install https://spark.apache.org/downloads.html

### AWS

#### IAM

Python EMR Runner requires AWS IAM user with following policies:

* AmazonElasticMapReduceFullAccess
* AmazonS3FullAccess

The user credentials should be put to ~/.aws/credentials with name [emr_runner].

See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html for more details.

#### S3

You need at least one S3-bucket set up and configured to environment variables to use Python EMR Runner.

## Tests

### Java

```bash
$ mvn test
```

### Python

```bash
$ conda activate hadoop-learning
$ pytest
```

## Data acquisition

### Data

This project uses same NCDC weather dataset that was used in [Hadoop Book](http://hadoopbook.com/).

In future I will hopefully do automatic data pipeline and experiment with formats such as Avro and Parquet.

### Download

Dataset is gz-packaged fixed width files, and I downloaded them with FTP from ftp://ftp.ncdc.noaa.gov/pub/data/noaa/, see scripts/redownload-ncnc-data.sh for example script.

### Preprocessiing

However, as the ftp-server has huge number of small files that do not play well with MapReduce and Spark, I made small utility to combine files to yearly:

```bash
$ conda activate hadoop-learning
$ cd src/main/python
$ python -m ncdc_analysis.cli.file_combiner --input <input-path> --output <output-path>
```

### To AWS

If you want to run example programs in EMR, you have to send the data to AWS S3. See e.g. scripts/ncdc-data-to-s3.sh

## Usage

### Running MapReduce locally

Note that currently this project is only to be able to run job that is marked as mainClass in pom.xml (Issue #5). There is a workaround for local hadoop, but not for EMR. 

#### Hadoop testing with single VM

````bash
$ hadoop jar target/hadoop-learning-0.1-shaded.jar -conf conf/hadoop-local.xml <input-folder-path> <output-folder-path>
````

#### Hadoop pseudocluster

To run job in [pseudocluster](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation) you need to first configure:

* Hadoop Filesystem
* Pseudocluster

I recommend consulting [Hadoop Book](http://hadoopbook.com/). 

```bash
$ ./scripts/start-hadoop.sh
$ hadoop jar target/hadoop-learning-0.1-shaded.jar -conf conf/hadoop-localhost.xml <input-folder-path> <output-folder-path>
```

Please note that in this case <input-folder-path> and <output-folder-path> are folders in hadoop filesystem.

#### Development

Classes in ncdc_analysis.map_reduce.*.*Driver.class can be also run as any java class.

### Running Spark locally

#### spark-submit

See example in scripts/run-spark-local.sh

#### Development

To run spark classes in your IDE, you need to add -Dspark.master=local[*] to your VM options, as normally master is given in spark-submit.

E.g. in IntelliJ VM options can be found from Run->Edit Configurations->VM Options

### Using EMR Runner

Python based EMR Runner (ncdc_analysis.cli.cluster_runner) can be used to start MapReduce and Spark jobs remotely in AWS's EMR cluster and after the execution retrieving results locally.

For optimal usage, see section Environment variables and .env.template

To see available options, use --help:

````bash
$ conda activate hadoop-learning
$ cd src/main/python
$ python -m ncdc_analysis.cli.cluster_runner --help

Usage: cluster_runner.py [OPTIONS]

Options:
  --job-type [mapreduce|spark]  EMR job type
  --jar-path TEXT               S3 path to .jar, defaults to env variable
                                NCDC_JARS_S3_PATH
  --jar-class TEXT              Class to run with Spark, not supported with
                                job-type mapreduce
  --packages TEXT               Extra packages provided for Spark (see. spark-
                                submit), separate packages with commas ','.
                                Not supported with job-type mapreduce
  --logs-path TEXT              S3 output path for logs, defaults to env
                                variable NCDC_LOGS_S3_PATH
  --input-data TEXT             Input data used for the job. Accepts following
                                parameters:
                                1) test (default) => Uses env
                                variable NCDC_LOGS_S3_DATA_TEST_PATH
                                2) prod
                                => Uses env variable
                                NCDC_LOGS_S3_DATA_PROD_PATH
                                3) other => Tries
                                use input_data as S3 path for input_data data
  --out-s3 TEXT                 S3 path used to output results, defaults to
                                env variable NCDC_S3_OUT_PATH
  --out-local TEXT              local path used to output results, defaults to
                                env variable NCDC_S3_OUT_PATH
  --instance-type TEXT          EMR instance type, used for master and slave
                                instances.
  --instance-count INTEGER      Number of instances used for the EMR cluster.
  --help                        Show this message and exit.
````

#### MapReduce example

````bash
$ python -m ncdc_analysis.cli.cluster_runner --job-type mapreduce --instance-count 3

Waiting until EMR job has been completed
Waiting for EMR-step Hadoop Jar Step
EMR Job completed!
Results fetched
````

#### Spark example

````bash
$ python -m ncdc_analysis.cli.cluster_runner --job-type spark --jar-class ncdc_analysis.spark.temperature.MaxTemperatureApp --packages com.databricks:spark-csv_2.11:1.5.0 --input-data test

Waiting until EMR job has been completed
Waiting for EMR-step Spark Jar Step
EMR Job completed!
Results fetched

````

## Authors

* Tomi Kasurinen

## Licence

MIT
