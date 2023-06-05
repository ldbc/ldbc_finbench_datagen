import sys
import os
import glob
import collections


def print_counts(counts):
    for key, value in collections.OrderedDict(sorted(counts.items())).items():
        print("{}:{}".format(key, value))


def count_entites(path):
    path = os.path.join(path, "raw")
    counts = {}
    for subdir in os.listdir(path):
        subdir_path = os.path.join(path, subdir)
        if os.path.isdir(subdir_path):
            num_entites = 0
            for file in glob.glob(os.path.join(subdir_path, "*.csv")):
                num_entites += sum(1 for _ in open(file)) - 1
            counts[subdir] = num_entites
    print_counts(counts)


if __name__ == "__main__":
    count_entites(sys.argv[1])
