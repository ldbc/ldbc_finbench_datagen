import hashlib
import os
import sys

print_templ = "| {} | {} | {} | {} |"

def check_consistency(dir1, dir2):
    subdirs1 = [d for d in os.listdir(dir1) if os.path.isdir(os.path.join(dir1, d))]
    subdirs2 = [d for d in os.listdir(dir2) if os.path.isdir(os.path.join(dir2, d))]
    common_subdirs = set(subdirs1) & set(subdirs2)

    headers = ["Subdir", "Dir1", "Dir2", "Consistency"]
    max_len0 = max(max([len(d) for d in common_subdirs]), len(headers[0]))
    max_len1 = max(len(dir1), len(headers[1]))
    max_len2 = max(len(dir2), len(headers[2]))
    max_len3 = max(len("same"), len(headers[3]))

    def align_print(col0: str, col1: str, col2: str, col3: str):
        print(print_templ.format(col0.center(max_len0), col1.center(max_len1), col2.center(max_len2),
                                 col3.center(max_len3)))

    align_print(headers[0], headers[1], headers[2], headers[3])
    for subdir in common_subdirs:
        files1 = [f for f in os.listdir(os.path.join(dir1, subdir)) if f.endswith('.csv')]
        files2 = [f for f in os.listdir(os.path.join(dir2, subdir)) if f.endswith('.csv')]
        assert (len(files1) == len(files2) and len(files1) == 1)
        path1 = os.path.join(dir1, subdir, files1[0])
        path2 = os.path.join(dir2, subdir, files2[0])
        with open(path1, 'rb') as f1, open(path2, 'rb') as f2:
            if hashlib.md5(f1.read()).hexdigest() == hashlib.md5(f2.read()).hexdigest():
                align_print(subdir, dir1, dir2, "same")
            else:
                align_print(subdir, dir1, dir2, "different")


if __name__ == '__main__':
    dir1 = sys.argv[1]
    dir2 = sys.argv[2]
    check_consistency(dir1, dir2)
