package ldbc.finbench.datagen.factors

import ldbc.finbench.datagen.util.DatagenStage
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, countDistinct, row_number, var_pop}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.collect_set
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.array_join
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.format_string
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.graphframes.GraphFrame
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser
import shapeless.lens

import scala.util.matching.Regex
import ldbc.finbench.datagen.factors.AccountItemsGenerator

object FactorGenerationStage extends DatagenStage {

  @transient lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  case class Args(
      outputDir: String = "out",
      irFormat: String = "parquet",
      format: String = "parquet",
      only: Option[Regex] = None,
      force: Boolean = false
  )

  override type ArgsType = Args

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Args](getClass.getName.dropRight(1)) {
      head(appName)

      val args = lens[Args]

      opt[String]('o', "output-dir")
        .action((x, c) => args.outputDir.set(c)(x))
        .text(
          "path on the cluster filesystem, where Datagen outputs. Can be a URI (e.g S3, ADLS, HDFS) or a " +
            "path in which case the default cluster file system is used."
        )

      opt[String]("ir-format")
        .action((x, c) => args.irFormat.set(c)(x))
        .text("Format of the raw input")

      opt[String]("format")
        .action((x, c) => args.format.set(c)(x))
        .text("Output format")

      opt[String]("only")
        .action((x, c) => args.only.set(c)(Some(x.r.anchored)))
        .text("Only generate factor tables whose name matches the supplied regex")

      opt[Unit]("force")
        .action((_, c) => args.force.set(c)(true))
        .text("Overwrites existing output")

      help('h', "help").text("prints this usage text")
    }
    val parsedArgs =
      parser.parse(args, Args()).getOrElse(throw new RuntimeException("Invalid arguments"))

    run(parsedArgs)
  }

  // execute factorization process
  // TODO: finish all
  override def run(args: Args) = {
    parameterCuration
  }

  def parameterCuration(implicit spark: SparkSession) = {
    import spark.implicits._

    // AccountItemsGenerator.generateAccountItems
    val accountDf = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/account/*.csv")

    val transferDf = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/transfer/*.csv")

    val withdrawDf = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/withdraw/*.csv")

    val combinedDf = transferDf.select($"fromId", $"toId", $"amount".cast("double"))
      .union(withdrawDf.select($"fromId", $"toId", $"amount".cast("double")))

    val transactionsDf = transferDf.select(col("fromId"), col("createTime"))
      .union(withdrawDf.select(col("fromId"), col("createTime")))

    val transactionsByMonthDf = transactionsDf
      .withColumn("year_month", date_format((col("createTime") / 1000).cast("timestamp"), "yyyy-MM"))
      .groupBy("fromId", "year_month")
      .count()

    val pivotDf = transactionsByMonthDf.groupBy("fromId")
      .pivot("year_month")
      .agg(first("count"))
      .na.fill(0) 
      .withColumnRenamed("fromId", "account_id")

    val maxAmountDf = combinedDf.groupBy($"fromId", $"toId")
      .agg(max($"amount").alias("maxAmount"))

    val accountItemsDf = maxAmountDf.groupBy($"fromId")
      .agg(F.collect_list(F.array($"toId", $"maxAmount")).alias("items"))
      .select($"fromId".alias("account_id"), $"items")
      .sort($"account_id")

    val transformedAccountItemsDf = accountItemsDf.withColumn(
      "items",
      F.expr("transform(items, array -> concat('[', concat_ws(',', array), ']'))")
    ).withColumn(
      "items",
      F.concat_ws(",", $"items")
    ).withColumn(
      "items",
      F.concat(lit("["), $"items", lit("]"))
    )

    val transactionsAmountDf = transferDf.select(col("fromId"), col("amount").cast("double"))
      .union(withdrawDf.select(col("fromId"), col("amount").cast("double")))

    val buckets = Array(10000, 30000, 100000, 300000, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000)

    val bucketedDf = transactionsAmountDf.withColumn("bucket", 
      when(col("amount") <= buckets(0), buckets(0))
        .when(col("amount") <= buckets(1), buckets(1))
        .when(col("amount") <= buckets(2), buckets(2))
        .when(col("amount") <= buckets(3), buckets(3))
        .when(col("amount") <= buckets(4), buckets(4))
        .when(col("amount") <= buckets(5), buckets(5))
        .when(col("amount") <= buckets(6), buckets(6))
        .when(col("amount") <= buckets(7), buckets(7))
        .when(col("amount") <= buckets(8), buckets(8))
        .when(col("amount") <= buckets(9), buckets(9))
        .when(col("amount") <= buckets(10), buckets(10))
        .when(col("amount") <= buckets(11), buckets(11))
        .when(col("amount") <= buckets(12), buckets(12))
        .otherwise(buckets(13))
    )

    val bucketCountsDf = bucketedDf.groupBy("fromId")
      .pivot("bucket", buckets.map(_.toString))
      .count()
      .na.fill(0)
      .withColumnRenamed("fromId", "account_id")

    val loanDf = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/loan/*.csv")

    val depositDf = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/deposit/*.csv")

    val loanAccountListDf = loanDf.join(depositDf, loanDf("id") === depositDf("loanId"), "left_outer")
      .groupBy("id")
      .agg(coalesce(collect_set("accountId"), array()).alias("account_list"))  
      .select(col("id").alias("loan_id"), concat(lit("["), array_join(col("account_list"), ","), lit("]")).alias("account_list"))  

    val transactionsSumDf = transferDf.select(col("toId"), col("amount").cast("double"))
      .union(withdrawDf.select(col("toId"), col("amount").cast("double")))

    val amountSumDf = transactionsSumDf.groupBy("toId")
      .agg(sum("amount").alias("amount"))
      .withColumnRenamed("toId", "account_id")
      .select(col("account_id"), format_string("%.2f", col("amount")).alias("amount"))  
    
    amountSumDf
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/new_factor_table/amount")

    loanAccountListDf
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/new_factor_table/loan_account_list")

    transformedAccountItemsDf
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/new_factor_table/account_items")

    
    pivotDf
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/new_factor_table/month")

    bucketCountsDf
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/new_factor_table/amount_bucket")

    
  }
}
