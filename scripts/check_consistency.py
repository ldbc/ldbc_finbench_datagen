import glob
import hashlib
import os
import sys

print_templ = "| {} | {} | {} | {} |"


def check_consistency(dir1, dir2, prefix):
    subdirs1 = [d for d in os.listdir(os.path.join(dir1, prefix)) if os.path.isdir(os.path.join(dir1, prefix, d))]
    subdirs2 = [d for d in os.listdir(os.path.join(dir2, prefix)) if os.path.isdir(os.path.join(dir2, prefix, d))]
    common_subdirs = set(subdirs1) & set(subdirs2)

    headers = ["Subdir", "Dir1", "Dir2", "Consistency"]
    max_len0 = max(max([len(prefix + "/" + d) for d in common_subdirs]), len(headers[0]))
    max_len1 = max(len(dir1), len(headers[1]))
    max_len2 = max(len(dir2), len(headers[2]))
    max_len3 = max(len("same"), len(headers[3]))

    def align_print(col0: str, col1: str, col2: str, col3: str):
        print(print_templ.format(col0.center(max_len0), col1.center(max_len1), col2.center(max_len2),
                                 col3.center(max_len3)))

    align_print(headers[0], headers[1], headers[2], headers[3])
    for subdir in common_subdirs:
        files1 = glob.glob("{}/{}/{}/*.csv".format(dir1, prefix, subdir))
        files2 = glob.glob("{}/{}/{}/*.csv".format(dir2, prefix, subdir))
        assert (len(files1) == len(files2) and len(files1) == 1)
        with open(files1[0], 'rb') as f1, open(files2[0], 'rb') as f2:
            if hashlib.md5(f1.read()).hexdigest() == hashlib.md5(f2.read()).hexdigest():
                align_print(prefix + "/" + subdir, dir1, dir2, "same")
            else:
                align_print(prefix + "/" + subdir, dir1, dir2, "different")


if __name__ == '__main__':
    dir1 = sys.argv[1]
    dir2 = sys.argv[2]
    check_consistency(dir1, dir2, "raw")
