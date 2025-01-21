#!/bin/bash
#
# Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


LDBC_FINBENCH_DATAGEN_JAR=target/ldbc_finbench_datagen-0.2.0-SNAPSHOT-jar-with-dependencies.jar
OUTPUT_DIR=out

# Note: generate factor tables with --generate-factors

# run locally with the python script
# time python3 scripts/run.py --jar $LDBC_FINBENCH_DATAGEN_JAR --main-class ldbc.finbench.datagen.LdbcDatagen --memory 500g -- --scale-factor 30 --output-dir ${OUTPUT_DIR}

# run locally with spark-submit command
# **({'spark.driver.extraJavaOptions': '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005'}), # Debug
# **({'spark.executor.extraJavaOptions': '-verbose:gc -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps'}),
# --conf "spark.memory.offHeap.enabled=true" \
# --conf "spark.memory.offHeap.size=100g" \
# --conf "spark.storage.memoryFraction=0" \
# --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \

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

# currently works on SF100
#time spark-submit --master local[*] \
#    --class ldbc.finbench.datagen.LdbcDatagen \
#    --driver-memory 400g \
#    --conf "spark.default.parallelism=800" \
#    --conf "spark.shuffle.compress=true" \
#    --conf "spark.shuffle.spill.compress=true" \
#    --conf "spark.kryoserializer.buffer.max=512m" \
#    --conf "spark.driver.maxResultSize=0" \
#    --conf "spark.driver.extraJavaOptions=-Xss512m" \
#    --conf "spark.executor.extraJavaOptions=-Xss512m -XX:+UseG1GC" \
#    --conf "spark.kryo.referenceTracking=false" \
#    ${LDBC_FINBENCH_DATAGEN_JAR} \
#    --scale-factor 100 \
#    --output-dir ${OUTPUT_DIR}

