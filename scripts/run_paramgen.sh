#!/bin/bash

LDBC_FINBENCH_DATAGEN_JAR=target/ldbc_finbench_datagen-0.2.0-SNAPSHOT-jar-with-dependencies.jar
OUTPUT_DIR=out/sf3/

# Note: generate factor tables with --generate-factors

echo "start factor table generation"

time spark-submit --master local[*] \
    --class ldbc.finbench.datagen.LdbcDatagen \
    --driver-memory 480g \
    ${LDBC_FINBENCH_DATAGEN_JAR} \
    --output-dir ${OUTPUT_DIR} \
    --factor-format csv \
    --generate-factors

echo "start parameter curation"