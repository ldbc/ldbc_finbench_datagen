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
# --conf "spark.memory.offHeap.enabled=true" \
# --conf "spark.memory.offHeap.size=100g" \
time spark-submit --master spark://finbench-large-00:7077 \
    --class ldbc.finbench.datagen.LdbcDatagen \
    --num-executors 2 \
    --conf "spark.default.parallelism=800" \
    --conf "spark.network.timeout=100000" \
    --conf "spark.shuffle.compress=true" \
    --conf "spark.shuffle.spill.compress=true" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.driver.memory=100g" \
    --conf "spark.driver.maxResultSize=0" \
    --conf "spark.executor.memory=400g" \
    --conf "spark.executor.memoryOverheadFactor=0.5" \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
    ${LDBC_FINBENCH_DATAGEN_JAR} \
    --scale-factor 100 \
    --output-dir ${OUTPUT_DIR}

echo "End: " `date`
