#!/bin/bash

LDBC_FINBENCH_DATAGEN_JAR=target/ldbc_finbench_datagen-0.2.0-SNAPSHOT-jar-with-dependencies.jar
OUTPUT_DIR=out

# run locally with the python script
# time python3 scripts/run.py --jar $LDBC_FINBENCH_DATAGEN_JAR --main-class ldbc.finbench.datagen.LdbcDatagen --memory 500g -- --scale-factor 30 --output-dir ${OUTPUT_DIR}

# run locally with spark-submit command
# **({'spark.driver.extraJavaOptions': '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005'}), # Debug
# **({'spark.executor.extraJavaOptions': '-verbose:gc -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps'}),
time spark-submit --master local[*] \
    --class ldbc.finbench.datagen.LdbcDatagen \
    --driver-memory 480g \
    --conf "spark.default.parallelism=500" \
    --conf "spark.shuffle.compress=true" \
    --conf "spark.shuffle.spill.compress=true" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.memory.offHeap.enabled=true" \
    --conf "spark.memory.offHeap.size=100g" \
    --conf "spark.storage.memoryFraction=0" \
    --conf "spark.driver.maxResultSize=0" \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
    ${LDBC_FINBENCH_DATAGEN_JAR} \
    --scale-factor 10 \
    --output-dir ${OUTPUT_DIR}
