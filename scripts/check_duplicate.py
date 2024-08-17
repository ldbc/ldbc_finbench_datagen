from pyspark.sql import SparkSession
import glob
import sys
import os

spark = SparkSession.builder.appName("check_dup").getOrCreate()


def check_dup(subdir, key):
    datas = []
    for csv in glob.glob(subdir + "/*.csv"):
        datas.append(
            spark.read.option("delimiter", "|").csv(csv, header=True, inferSchema=True)
        )

    merged = datas[0]
    for df in datas[1:]:
        merged = merged.unionAll(df)

    dups = merged.groupBy(key).count().filter("count > 1")
    print(
        "{}: Total rows: {}, duplicated {}".format(subdir, merged.count(), dups.count())
    )
    dups.show(5)


if __name__ == "__main__":
    prefix = sys.argv[1]
    check_dup(os.path.join(prefix, "account"), "id")
    check_dup(os.path.join(prefix, "company"), "id")
    check_dup(os.path.join(prefix, "person"), "id")
    check_dup(os.path.join(prefix, "medium"), "id")
