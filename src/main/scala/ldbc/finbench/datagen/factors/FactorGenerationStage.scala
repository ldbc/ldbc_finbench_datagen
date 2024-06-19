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
import org.apache.spark.sql.functions.collect_list
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

    val loanRDD = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/loan/*.csv")

    val depositRDD = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/deposit/*.csv")

    val personInvestRDD = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/personInvest/*.csv")

    val PersonInvestCompanyRDD = personInvestRDD
      .groupBy("investorId")
      .agg(countDistinct("companyId").alias("num"))
      .withColumnRenamed("investorId", "personId")

    val OwnRDD = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/personOwnAccount/*.csv")

    val personGuaranteeRDD = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/raw/personGuarantee/*.csv")

    val transferOutRDD = transferRDD.select($"fromId", $"toId")

    val transferOutLRDD = transferOutRDD.groupBy($"fromId")
      .agg(F.collect_list($"toId").alias("transfer_out_list"))
      .select($"fromId".alias("account_id"), $"transfer_out_list")
      .sort($"account_id")

    val transferOutListRDD = transferOutLRDD.withColumn(
      "transfer_out_list",
      F.concat(lit("["), F.concat_ws(",", $"transfer_out_list"), lit("]"))
    )

    val transferInRDD = transferRDD.select($"toId", $"fromId")

    val transferInLRDD = transferInRDD.groupBy($"toId")
      .agg(F.collect_list($"fromId").alias("transfer_in_list"))
      .select($"toId".alias("account_id"), $"transfer_in_list")
      .sort($"account_id")

    val transferInListRDD = transferInLRDD.withColumn(
      "transfer_in_list",
      F.concat(lit("["), F.concat_ws(",", $"transfer_in_list"), lit("]"))
    )

    val transferInTimeRDD = transferRDD.select(col("toId"), col("createTime"))

    val transferInByMonthRDD = transferInTimeRDD
      .withColumn("year_month", date_format((col("createTime") / 1000).cast("timestamp"), "yyyy-MM"))
      .groupBy("toId", "year_month")
      .count()

    val transferInPivotRDD = transferInByMonthRDD.groupBy("toId")
      .pivot("year_month")
      .agg(first("count"))
      .na.fill(0)
      .withColumnRenamed("toId", "account_id")

    val InvestInTimeRDD = personInvestRDD.select(col("investorId"), col("createTime"))

    val InvestInByMonthRDD = InvestInTimeRDD
      .withColumn("year_month", date_format((col("createTime") / 1000).cast("timestamp"), "yyyy-MM"))
      .groupBy("investorId", "year_month")
      .count()

    val InvestInPivotRDD = InvestInByMonthRDD.groupBy("investorId")
      .pivot("year_month")
      .agg(first("count"))
      .na.fill(0)
      .withColumnRenamed("investorId", "person_id")

    val transferOutTimeRDD = transferRDD.select(col("fromId"), col("createTime"))

    val transferOutByMonthRDD = transferOutTimeRDD
      .withColumn("year_month", date_format((col("createTime") / 1000).cast("timestamp"), "yyyy-MM"))
      .groupBy("fromId", "year_month")
      .count()

    val transferOutPivotRDD = transferOutByMonthRDD.groupBy("fromId")
      .pivot("year_month")
      .agg(first("count"))
      .na.fill(0)
      .withColumnRenamed("fromId", "account_id")

    val withdrawInTimeRDD = withdrawRDD.select(col("toId"), col("createTime"))

    val withdrawInByMonthRDD = withdrawInTimeRDD
      .withColumn("year_month", date_format((col("createTime") / 1000).cast("timestamp"), "yyyy-MM"))
      .groupBy("toId", "year_month")
      .count()

    val withdrawInPivotRDD = withdrawInByMonthRDD.groupBy("toId")
      .pivot("year_month")
      .agg(first("count"))
      .na.fill(0)
      .withColumnRenamed("toId", "account_id")
      
    val withdrawInRDD = withdrawRDD.select($"toId", $"fromId", $"amount".cast("double"))

    val combinedRDD = transferRDD.select($"fromId", $"toId", $"amount".cast("double"))
      .union(withdrawRDD.select($"fromId", $"toId", $"amount".cast("double")))

    val transactionsRDD = transferRDD.select(col("fromId"), col("createTime"))
      .union(withdrawRDD.select(col("fromId"), col("createTime")))

    val transactionsByMonthRDD = transactionsRDD
      .withColumn("year_month", date_format((col("createTime") / 1000).cast("timestamp"), "yyyy-MM"))
      .groupBy("fromId", "year_month")
      .count()

    val pivotRDD = transactionsByMonthRDD.groupBy("fromId")
      .pivot("year_month")
      .agg(first("count"))
      .na.fill(0) 
      .withColumnRenamed("fromId", "account_id")

    val maxAmountRDD = combinedRDD.groupBy($"fromId", $"toId")
      .agg(max($"amount").alias("maxAmount"))

    val withdrawInAmountRDD = withdrawInRDD.groupBy($"toId", $"fromId")
      .agg(max($"amount").alias("maxAmount"))

    val accountItemsRDD = maxAmountRDD.groupBy($"fromId")
      .agg(F.collect_list(F.array($"toId", $"maxAmount")).alias("items"))
      .select($"fromId".alias("account_id"), $"items")
      .sort($"account_id")
    
    val withdrawInItemsRDD = withdrawInAmountRDD.groupBy($"toId")
      .agg(F.collect_list(F.array($"fromId", $"maxAmount")).alias("items"))
      .select($"toId".alias("account_id"), $"items")
      .sort($"account_id")

    val transformedAccountItemsRDD = accountItemsRDD.withColumn(
      "items",
      F.expr("transform(items, array -> concat('[', concat_ws(',', array), ']'))")
    ).withColumn(
      "items",
      F.concat_ws(",", $"items")
    ).withColumn(
      "items",
      F.concat(lit("["), $"items", lit("]"))
    )

    val transformedWithdrawInItemsRDD = withdrawInItemsRDD.withColumn(
      "items",
      F.expr("transform(items, array -> concat('[', concat_ws(',', array), ']'))")
    ).withColumn(
      "items",
      F.concat_ws(",", $"items")
    ).withColumn(
      "items",
      F.concat(lit("["), $"items", lit("]"))
    )

    val transactionsAmountRDD = transferRDD.select(col("fromId"), col("amount").cast("double"))
      .union(withdrawRDD.select(col("fromId"), col("amount").cast("double")))

    val buckets = Array(10000, 30000, 100000, 300000, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000)

    val bucketRDD = transactionsAmountRDD.withColumn("bucket", 
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

    val bucketCountsRDD = bucketRDD.groupBy("fromId")
      .pivot("bucket", buckets.map(_.toString))
      .count()
      .na.fill(0)
      .withColumnRenamed("fromId", "account_id")

    val transferInAmountRDD = transferRDD.select($"toId", $"amount".cast("double"))

    val transferInbucketRDD = transferInAmountRDD.withColumn("bucket", 
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

    val transferInBucketCountsRDD = transferInbucketRDD.groupBy("toId")
      .pivot("bucket", buckets.map(_.toString))
      .count()
      .na.fill(0)
      .withColumnRenamed("toId", "account_id")

    val transferOutAmountRDD = transferRDD.select($"fromId", $"amount".cast("double"))

    val transferOutbucketRDD = transferOutAmountRDD.withColumn("bucket", 
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

    val transferOutBucketCountsRDD = transferOutbucketRDD.groupBy("fromId")
      .pivot("bucket", buckets.map(_.toString))
      .count()
      .na.fill(0)
      .withColumnRenamed("fromId", "account_id")

    val PersonOwnAccountRDD = OwnRDD.select($"personId", $"accountId")
      .groupBy("personId")
      .agg(coalesce(collect_set("accountId"), array()).alias("account_list"))
      .select(col("personId").alias("person_id"), concat(lit("["), array_join(col("account_list"), ","), lit("]")).alias("account_list"))
      .orderBy("person_id")

    val loanAccountListRDD = loanRDD.join(depositRDD, loanRDD("id") === depositRDD("loanId"), "left_outer")
      .groupBy("id")
      .agg(coalesce(collect_set("accountId"), array()).alias("account_list"))  
      .select(col("id").alias("loan_id"), concat(lit("["), array_join(col("account_list"), ","), lit("]")).alias("account_list"))  
      .orderBy("loan_id")

    val transactionsSumRDD = transferRDD.select(col("toId"), col("amount").cast("double"))
      .union(withdrawRDD.select(col("toId"), col("amount").cast("double")))

    val amountSumRDD = transactionsSumRDD.groupBy("toId")
      .agg(sum("amount").alias("amount"))
      .withColumnRenamed("toId", "account_id")
      .select(col("account_id"), format_string("%.2f", col("amount")).alias("amount"))  
    
    amountSumRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/upstream_amount")

    loanAccountListRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/loan_account_list")

    PersonOwnAccountRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/person_account_list")

    PersonInvestCompanyRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/person_invest_company")

    transformedAccountItemsRDD
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/trans_withdraw_items")

    transformedWithdrawInItemsRDD
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/account_withdraw_in_items")
    
    pivotRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/trans_withdraw_month")

    transferInPivotRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/transfer_in_month")

    transferOutPivotRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/transfer_out_month")

    InvestInPivotRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/invest_month")

    withdrawInPivotRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/withdraw_in_month")

    bucketCountsRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/trans_withdraw_bucket")

    transferOutBucketCountsRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/transfer_out_bucket")

    transferInBucketCountsRDD
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/transfer_in_bucket")

    transferOutListRDD
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/account_transfer_out_list")

    transferInListRDD
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save("./out/factor_table/account_transfer_in_list")
  }
}
