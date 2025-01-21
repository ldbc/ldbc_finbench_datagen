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

import glob
import sys
import os.path

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check_time").getOrCreate()

subdirs = [
    "account",
    "companyOwnAccount",
    "withdraw",
    "deposit",
    "loantransfer",
    "personOwnAccount",
    "signIn",
    "repay",
    "transfer",
]


def read_data(path):
    dataframes = [
        spark.read.option("delimiter", "|").csv(csv, header=True, inferSchema=True)
        for csv in glob.glob(path)
    ]
    allTransfer = dataframes[0]
    for idx, dataframe in enumerate(dataframes):
        if idx == 0:
            continue
        allTransfer = allTransfer.union(dataframe)
    return allTransfer


if __name__ == "__main__":
    prefix = sys.argv[1]
    for subdir in subdirs:
        print("Checking {} if deletion before creation......".format(subdir))
        if not os.path.exists(os.path.join(prefix, subdir)):
            print("No {} data exists!\n".format(subdir))
            continue
        data = read_data(os.path.join(prefix, subdir, "*.csv"))
        wrong = data.filter(data["createTime"] >= data["deleteTime"])
        if wrong.count() > 0:
            print(
                "{} invalid! Having {} rows with wrong time\n".format(
                    subdir, wrong.count()
                )
            )
            wrong.show(3)
        else:
            print("{} passed.\n".format(subdir))
