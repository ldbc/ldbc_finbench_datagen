#!/bin/bash

LDBC_FINBENCH_DATAGEN_JAR=target/ldbc_finbench_datagen-0.2.0-SNAPSHOT-jar-with-dependencies.jar
OUTPUT_DIR=/tmp/finbench-out/

echo "start: " `date`

# Run Spark Application
#spark-submit --class org.apache.spark.examples.SparkPi  --master spark://finbench-large-00:7077 --num-executors 2
#--conf "spark.default.parallelism=2" ~/software/spark-3.2.2-bin-hadoop3.2/examples/jars/spark-examples_2.12-3.2.2.jar
#10000

time spark-submit --master spark://finbench-large-00:7077 --class ldbc.finbench.datagen.LdbcDatagen ${LDBC_FINBENCH_DATAGEN_JAR} --scale-factor 1 --output-dir ${OUTPUT_DIR}

#spark-submit --master spark://finbench-large-00:7077 --class ldbc.finbench.datagen.LdbcDatagen --num-executors 2 --conf "spark.default.parallelism=2"   /home/qsp/github-projects/ldbc_finbench_datagen/target/ldbc_finbench_datagen-0.2.0-SNAPSHOT-jar-with-dependencies.jar --scale-factor 1 --output-dir /tmp/finbench-out/

echo "End: " `date`
