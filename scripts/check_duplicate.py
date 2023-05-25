from pyspark.sql import SparkSession
import glob
import sys

spark = SparkSession.builder.appName("check_dup").getOrCreate()


def check_dup(subdir, key):
    datas = []
    for csv in glob.glob(subdir + "/*.csv"):
        datas.append(spark.read.option("delimiter", "|").csv(csv, header=True, inferSchema=True))

    merged = datas[0]
    for df in datas[1:]:
        merged = merged.unionAll(df)

    print("Total rows: ", merged.count())
    merged.groupBy(key).count().filter("count > 1").show(5)



if __name__ == "__main__":
    check_dup("out/account", "id")
