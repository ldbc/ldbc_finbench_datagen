#!/usr/bin/env bash
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


## Point this path to the directory containing the `raw` directory
FinBench_DATA_ROOT=${PATH_TO_FINBENCH_DATA}

set -eu
set -o pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

rm -rf ${FinBench_DATA_ROOT}/deletes ${FinBench_DATA_ROOT}/inserts
mkdir ${FinBench_DATA_ROOT}/deletes ${FinBench_DATA_ROOT}/inserts

echo "##### Transform to snapshots and write queries #####"
echo
echo "\${FinBench_DATA_ROOT}: ${FinBench_DATA_ROOT}"
echo

python3 ./convert_data.py --raw_dir ${FinBench_DATA_ROOT} --output_dir ${FinBench_DATA_ROOT} | tee output.log
