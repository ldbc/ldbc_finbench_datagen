#!/bin/bash

LDBC_FINBENCH_DATAGEN_JAR=target/ldbc_finbench_datagen-0.2.0-SNAPSHOT-jar-with-dependencies.jar
OUTPUT_DIR=/tmp/finbench-out/

echo "start: " `date`

# Run Spark Application

#--conf "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2" \
# --num-executors 2 \
# --conf "spark.shuffle.service.enabled=true"  \
# --conf "spark.dynamicAllocation.enabled=true" \
# --conf "spark.dynamicAllocation.minExecutors=1" \
# --conf "spark.dynamicAllocation.maxExecutors=10" \

# --conf "spark.yarn.maximizeResourceAllocation=true" \
time spark-submit --master spark://finbench-large-00:7077 \
    --class ldbc.finbench.datagen.LdbcDatagen \
    --num-executors 2 \
    --conf "spark.default.parallelism=400" \
    --conf "spark.network.timeout=100000" \
    --conf "spark.shuffle.compress=true" \
    --conf "spark.shuffle.spill.compress=true" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.driver.memory=200g" \
    --conf "spark.driver.maxResultSize=10g" \
    --conf "spark.executor.memory=300g" \
    --conf "spark.executor.memoryOverheadFactor=0.2" \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
    ${LDBC_FINBENCH_DATAGEN_JAR} \
    --scale-factor 10 \
    --output-dir ${OUTPUT_DIR}

echo "End: " `date`
