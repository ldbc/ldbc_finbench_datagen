import glob
import os.path

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check_time").getOrCreate()

subdirs = ["account", "companyOwnAccount", "withdraw", "deposit", "loantransfer", "personOwnAccount", "signIn", "repay",
           "transfer"]


def read_data(path):
    dataframes = [spark.read.option("delimiter", "|").csv(csv, header=True, inferSchema=True) for csv in
                  glob.glob(path)]
    allTransfer = dataframes[0]
    for idx, dataframe in enumerate(dataframes):
        if idx == 0:
            continue
        allTransfer = allTransfer.union(dataframe)
    return allTransfer


if __name__ == "__main__":
    prefix = "./out/"
    for subdir in subdirs:
        print("Checking {} if deletion before creation......".format(subdir))
        if not os.path.exists(prefix + subdir):
            print("No {} data exists!\n".format(subdir))
            continue
        data = read_data(prefix + subdir + "/*.csv")
        wrong = data.filter(data["createTime"] >= data["deleteTime"])
        if wrong.count() > 0:
            print("{} invalid! Having {} rows with wrong time\n".format(subdir, wrong.count()))
            wrong.show(3)
        else:
            print("{} passed.\n".format(subdir))