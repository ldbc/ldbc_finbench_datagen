package ldbc.finbench.datagen.factors

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.lit

object AccountItemsGenerator {
  def generateAccountItems(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val accountRDD = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/account/*.csv")

    val transferRDD = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/transfer/*.csv")

    val withdrawRDD = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/withdraw/*.csv")

    val combinedRDD = transferRDD
      .select($"fromId", $"toId", $"amount".cast("double"))
      .union(withdrawRDD.select($"fromId", $"toId", $"amount".cast("double")))

    val maxAmountRDD = combinedRDD
      .groupBy($"fromId", $"toId")
      .agg(max($"amount").alias("maxAmount"))

    val accountItemsRDD = maxAmountRDD
      .groupBy($"fromId")
      .agg(F.collect_list(F.array($"toId", $"maxAmount")).alias("items"))
      .select($"fromId".alias("account_id"), $"items")
      .sort($"account_id")

    val transformedAccountItemsRDD = accountItemsRDD
      .withColumn(
        "items",
        F.expr(
          "transform(items, array -> concat('[', concat_ws(',', array), ']'))"
        )
      )
      .withColumn(
        "items",
        F.concat_ws(",", $"items")
      )
      .withColumn(
        "items",
        F.concat(lit("["), $"items", lit("]"))
      )

    transformedAccountItemsRDD
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/account_items")
  }
}
