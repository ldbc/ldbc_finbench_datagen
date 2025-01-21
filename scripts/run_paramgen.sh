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
OUTPUT_DIR=out/

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