#!/usr/bin/env bash

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
