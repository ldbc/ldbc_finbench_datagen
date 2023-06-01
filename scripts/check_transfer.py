import glob
from collections import Counter

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, countDistinct

spark = SparkSession.builder.appName("profile").getOrCreate()


def readAllEdges(path):
    dataframes = [spark.read.option("delimiter", "|").csv(csv, header=True, inferSchema=True) for csv in
                  glob.glob(path)]
    allTransfer = dataframes[0]
    for idx, dataframe in enumerate(dataframes):
        if idx == 0:
            continue
        allTransfer = allTransfer.union(dataframe)
    return allTransfer


def draw_degree(inDegrees, outDegrees):
    def count_degree(degree: list):
        freq = Counter(degree).most_common()
        degrees = []
        counts = []
        for deg, count in freq:
            degrees.append(deg)
            counts.append(count)
        return degrees, counts

    inDegreeX, inDegreeY = count_degree(inDegrees)
    outDegreeX, outDegreeY = count_degree(outDegrees)
    xMax = max(max(inDegreeX), max(outDegreeX))
    yMax = max(max(inDegreeY), max(outDegreeY))

    fig = plt.figure()
    ind = fig.add_subplot(1, 2, 1)
    ind.set_title("In-Degree Distribution")
    ind.set_xlabel("InDegree")
    ind.set_ylabel("Count")
    ind.set_xlim(0, xMax)
    ind.set_ylim(0, yMax)
    ind.plot(inDegreeX, inDegreeY, color='blue', label='inDegree')
    ind.legend()

    outd = fig.add_subplot(1, 2, 2)
    outd.set_title("Out-Degree Distribution")
    outd.set_xlabel("OutDegree")
    outd.set_ylabel("Count")
    outd.set_xlim(0, xMax)
    outd.set_ylim(0, yMax)
    outd.plot(outDegreeX, outDegreeY, color='red', label='outDegree')
    outd.legend()

    plt.plot()
    plt.show()


# You can use this script to check if the in-degree and out-degree of each account is correct
if __name__ == "__main__":
    allTransferEdges = readAllEdges("./out/raw/transfer/*.csv")

    in_degrees = allTransferEdges.groupBy("toId").count().withColumnRenamed("count", "in_degree")
    out_degrees = allTransferEdges.groupBy("fromId").count().withColumnRenamed("count", "out_degree")
    multiplicity = allTransferEdges.groupBy("fromId", "toId").agg(countDistinct("multiplicityId").alias("multiplicity"))

    # Show top 20
    in_degrees.orderBy(desc("in_degree")).show(10)
    out_degrees.orderBy(desc("out_degree")).show(10)
    multiplicity.orderBy(desc("multiplicity")).show(10)

    # Calculate and print metrics
    numAccounts = allTransferEdges.select("fromId").union(allTransferEdges.select("toId")).distinct().count()
    numTransfers = allTransferEdges.count()
    demultipled = multiplicity.count()
    print("Num of accounts: {}".format(numAccounts))
    print("Num of transfer edges: {}".format(numTransfers))
    print("Average Degree: {}".format(numTransfers * 1.0 / numAccounts))
    print("Average Multiplicity: {}".format(numTransfers * 1.0 / demultipled))

    # Draw powerlaw distribution of the degrees
    inDegrees = [row["in_degree"] for row in in_degrees.select("in_degree").collect()]
    outDegrees = [row["out_degree"] for row in out_degrees.select("out_degree").collect()]
    draw_degree(inDegrees, outDegrees)
