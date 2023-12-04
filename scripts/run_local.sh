#!/bin/bash

LDBC_SNB_DATAGEN_JAR=target/ldbc_finbench_datagen-0.1.0-alpha-jar-with-dependencies.jar
OUTPUT_DIR=out

# For more command line arguments, see the main entry for more information at
# src/main/scala/ldbc/finbench/datagen/LdbcDatagen.scala
python3 scripts/run.py --jar $LDBC_SNB_DATAGEN_JAR --main-class ldbc.finbench.datagen.LdbcDatagen --memory 10g -- --scale-factor 0.1 --output-dir ${OUTPUT_DIR}
