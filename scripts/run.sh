#!/bin/bash

LDBC_SNB_DATAGEN_JAR=target/ldbc_finbench_datagen-0.1.0-alpha-jar-with-dependencies.jar

python3 scripts/run.py --jar $LDBC_SNB_DATAGEN_JAR

