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

import sys
import os
import glob
import collections


labels = ["person","personOwnAccount","personApplyLoan","personGuarantee","personInvest","blank","company","companyOwnAccount","companyApplyLoan","companyGuarantee","companyInvest","blank","account","transfer","withdraw","blank","loan","loantransfer","deposit","repay","blank","medium","signIn"]

def print_original_counts(counts):
    for key, value in collections.OrderedDict(sorted(counts.items())).items():
        print("{}:{}".format(key, value))

def print_formatted_counts(counts):
    for label in labels:
        if label == "blank":
            print("================================")
        else:
            print("{}:{}".format(label, counts[label]))

def count_entites(path):
    counts = {}
    for subdir in os.listdir(path):
        subdir_path = os.path.join(path, subdir)
        if os.path.isdir(subdir_path):
            num_entites = 0
            for file in glob.glob(os.path.join(subdir_path, "*.csv")):
                num_entites += sum(1 for _ in open(file)) - 1
            counts[subdir] = num_entites
    print_original_counts(counts)
    print("\n========== Formatted Output ============\n")
    print_formatted_counts(counts)


if __name__ == "__main__":
    count_entites(sys.argv[1])
