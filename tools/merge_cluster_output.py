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

import os
import sys


def merge_cluster_output(dir_A, dir_B, output_dir):
    # check if the directories exist
    assert os.path.exists(dir_A), "The directory {} does not exist".format(dir_A)
    assert os.path.exists(dir_B), "The directory {} does not exist".format(dir_B)
    # create the output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    # get all subdirectories in dir_A
    subdirs = [o for o in os.listdir(dir_A) if os.path.isdir(os.path.join(dir_A, o))]
    for subdir in subdirs:
        # check if the subdirectory exists in both dir_A and dir_B
        assert os.path.exists(
            os.path.join(dir_B, subdir)
        ), "The subdirectory {} does not exist in {}".format(subdir, dir_B)

    for subdir in subdirs:
        # get all csv files in the subdirectory
        csv_files_A = [
            os.path.join(dir_A, subdir, f)
            for f in os.listdir(os.path.join(dir_A, subdir))
            if f.endswith(".csv")
        ]
        csv_files_B = [
            os.path.join(dir_B, subdir, f)
            for f in os.listdir(os.path.join(dir_B, subdir))
            if f.endswith(".csv")
        ]

        # create a new directory in the output directory with the same name as the subdirectory
        new_subdir = os.path.join(output_dir, subdir)
        os.makedirs(new_subdir, exist_ok=True)
        # copy the csv files from dir_A to the new subdirectory
        for csv_file in csv_files_A:
            new_csv_file = os.path.join(new_subdir, os.path.basename(csv_file))
            os.system("cp {} {}".format(csv_file, new_csv_file))
        # copy the csv files from dir_B to the new subdirectory
        for csv_file in csv_files_B:
            new_csv_file = os.path.join(new_subdir, os.path.basename(csv_file))
            os.system("cp {} {}".format(csv_file, new_csv_file))


if __name__ == "__main__":
    dir_A = sys.argv[1]
    dir_B = sys.argv[2]
    output_dir = sys.argv[3]
    merge_cluster_output(dir_A, dir_B, output_dir)
