import hashlib
import os
import sys
import glob

print_templ = "| {} | {} | {} | {} |"


def get_md5_list(subdir, dir):
    md5_list = []
    csvs = glob.glob("{}/{}/*.csv".format(dir, subdir))
    for csv in csvs:
        with open(csv, 'rb') as f:
            md5_list.append(hashlib.md5(f.read()).hexdigest())
    return sorted(md5_list)

def check_multiple_files(subdir, dir1, dir2):
    dir1_list = get_md5_list(subdir, dir1)
    dir2_list = get_md5_list(subdir, dir2)
    return dir1_list == dir2_list


def check_consistency(dir1, dir2):
    subdirs1 = [d for d in os.listdir(dir1) if os.path.isdir(os.path.join(dir1, d))]
    subdirs2 = [d for d in os.listdir(dir2) if os.path.isdir(os.path.join(dir2, d))]
    common_subdirs = set(subdirs1) & set(subdirs2)

    headers = ["Subdir", "Dir1", "Dir2", "Consistency"]
    max_len0 = max(max([len(d) for d in common_subdirs]), len(headers[0]))
    max_len1 = max(len(dir1), len(headers[1]))
    max_len2 = max(len(dir2), len(headers[2]))
    max_len3 = max(len("same"), len("different"), len("skipped for more than one file"), len(headers[3]))

    def align_print(col0: str, col1: str, col2: str, col3: str):
        print(print_templ.format(col0.center(max_len0), col1.center(max_len1), col2.center(max_len2),
                                 col3.center(max_len3)))

    align_print(headers[0], headers[1], headers[2], headers[3])
    for subdir in sorted(common_subdirs):
        if check_multiple_files(subdir, dir1, dir2):
            align_print(subdir, dir1, dir2, "same")
        else:
            align_print(subdir, dir1, dir2, "different")


if __name__ == '__main__':
    dir1 = sys.argv[1]
    dir2 = sys.argv[2]
    check_consistency(dir1+"/raw", dir2+"/raw")
