/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
