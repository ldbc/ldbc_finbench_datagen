package ldbc.finbench.datagen.factors

import ldbc.finbench.datagen.util.DatagenStage
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser
import shapeless.lens

import scala.util.matching.Regex

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
        .text(
          "Only generate factor tables whose name matches the supplied regex"
        )

      opt[Unit]("force")
        .action((_, c) => args.force.set(c)(true))
        .text("Overwrites existing output")

      help('h', "help").text("prints this usage text")
    }
    val parsedArgs =
      parser
        .parse(args, Args())
        .getOrElse(throw new RuntimeException("Invalid arguments"))

    run(parsedArgs)
  }

  override def run(args: Args) = {
    factortables(args)
  }

  def readCSV(path: String): DataFrame = {
    val csvOptions = Map("header" -> "true", "delimiter" -> "|")
    spark.read
      .format("csv")
      .options(csvOptions)
      .load(path)
  }

  def transformItems(
      df: DataFrame,
      groupByCol: String,
      selectCol: String
  ): DataFrame = {
    val itemAmountRDD = df
      .groupBy(groupByCol, selectCol)
      .agg(
        max(col("amount")).alias("maxAmount"),
        max(col("createTime")).alias("maxCreateTime")
      )

    val itemsRDD = itemAmountRDD
      .groupBy(groupByCol)
      .agg(
        collect_list(
          array(col(selectCol), col("maxAmount"), col("maxCreateTime"))
        ).alias("items")
      )

    itemsRDD
      .withColumn(
        "items",
        F.expr(
          "transform(items, array -> concat('[', concat_ws(',', array), ']'))"
        )
      )
      .withColumn(
        "items",
        F.concat(lit("["), F.concat_ws(",", col("items")), lit("]"))
      )
  }

  def bucketAndCount(
      df: DataFrame,
      idCol: String,
      amountCol: String,
      groupCol: String
  ): DataFrame = {

    val buckets = Array(10000, 30000, 100000, 300000, 1000000, 2000000, 3000000,
      4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000)

    val bucketedRDD = df.withColumn(
      "bucket",
      when(col(amountCol) <= buckets(0), buckets(0))
        .when(col(amountCol) <= buckets(1), buckets(1))
        .when(col(amountCol) <= buckets(2), buckets(2))
        .when(col(amountCol) <= buckets(3), buckets(3))
        .when(col(amountCol) <= buckets(4), buckets(4))
        .when(col(amountCol) <= buckets(5), buckets(5))
        .when(col(amountCol) <= buckets(6), buckets(6))
        .when(col(amountCol) <= buckets(7), buckets(7))
        .when(col(amountCol) <= buckets(8), buckets(8))
        .when(col(amountCol) <= buckets(9), buckets(9))
        .when(col(amountCol) <= buckets(10), buckets(10))
        .when(col(amountCol) <= buckets(11), buckets(11))
        .when(col(amountCol) <= buckets(12), buckets(12))
        .otherwise(buckets(13))
    )

    bucketedRDD
      .groupBy(idCol)
      .pivot("bucket", buckets.map(_.toString))
      .count()
      .na
      .fill(0)
      .withColumnRenamed(idCol, groupCol)
  }

  def processByMonth(
      df: DataFrame,
      idCol: String,
      timeCol: String,
      newIdColName: String
  ): DataFrame = {
    val byMonthRDD = df
      .withColumn(
        "year_month",
        date_format((col(timeCol) / 1000).cast("timestamp"), "yyyy-MM")
      )
      .groupBy(idCol, "year_month")
      .count()

    val timestampedByMonthRDD = byMonthRDD
      .withColumn(
        "year_month_ts",
        unix_timestamp(col("year_month"), "yyyy-MM") * 1000
      )
      .drop("year_month")

    val pivotRDD = timestampedByMonthRDD
      .groupBy(idCol)
      .pivot("year_month_ts")
      .agg(first("count"))
      .na
      .fill(0)
      .withColumnRenamed(idCol, newIdColName)

    pivotRDD
  }

  def factortables(args: Args)(implicit spark: SparkSession) = {
//    import spark.implicits._
    log.info("[Main] Starting factoring stage")

    val transferRDD = readCSV(s"${args.outputDir}/raw/transfer/*.csv")
      .select(
        col("fromId"),
        col("toId"),
        col("amount").cast("double"),
        col("createTime")
      )

    val withdrawRDD = readCSV(s"${args.outputDir}/raw/withdraw/*.csv")
      .select(
        col("fromId"),
        col("toId"),
        col("amount").cast("double"),
        col("createTime")
      )

    val depositRDD = readCSV(s"${args.outputDir}/raw/deposit/*.csv")
      .select(col("accountId"), col("loanId"))

    val personInvestRDD = readCSV(s"${args.outputDir}/raw/personInvest/*.csv")
      .select(col("investorId"), col("companyIdstorId"), col("createTime"))

    val ownRDD = readCSV(s"${args.outputDir}/raw/personOwnAccount/*.csv")
      .select(col("personId"), col("accountId"))

    val personGuaranteeRDD =
      readCSV(s"${args.outputDir}/raw/personGuarantee/*.csv")
        .select(col("fromId"), col("toId"), col("createTime"))

    val PersonInvestCompanyRDD = personInvestRDD
      .groupBy("investorId")
      .agg(countDistinct("companyId").alias("num"))
      .withColumnRenamed("investorId", "personId")

    PersonInvestCompanyRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/person_invest_company")

    val transferItRDD =
      transferRDD.select(
        col("fromId"),
        col("toId"),
        col("amount").cast("double"),
        col("createTime")
      )

    val transferOutAccountItemsRDD =
      transformItems(transferItRDD, "fromId", "toId")

    transferOutAccountItemsRDD
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/account_transfer_out_items")

    val transferInAccountItemsRDD =
      transformItems(transferItRDD, "toId", "fromId")

    transferInAccountItemsRDD
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/account_transfer_in_items")

    val transferOutLRDD = transferItRDD
      .select(col("fromId"), col("toId"))
      .groupBy(col("fromId"))
      .agg(F.collect_list(col("toId")).alias("transfer_out_list"))
      .select(col("fromId").alias("account_id"), col("transfer_out_list"))

    val transferOutListRDD = transferOutLRDD.withColumn(
      "transfer_out_list",
      F.concat(lit("["), F.concat_ws(",", col("transfer_out_list")), lit("]"))
    )

    transferOutListRDD
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/account_transfer_out_list")

    val fromAccounts = transferRDD.select(
      col("fromId").alias("account_id"),
      col("toId").alias("corresponding_account_id"),
      col("createTime")
    )
    val toAccounts = transferRDD.select(
      col("toId").alias("account_id"),
      col("fromId").alias("corresponding_account_id"),
      col("createTime")
    )
    val allAccounts = fromAccounts.union(toAccounts)

    val accountListRDD = allAccounts
      .select("account_id", "corresponding_account_id")
      .groupBy("account_id")
      .agg(collect_set("corresponding_account_id").alias("account_list"))

    val accountCountDF = accountListRDD
      .select(col("account_id"), F.size(col("account_list")).alias("sum"))

    accountCountDF.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/account_in_out_count")

    val transferAccountListRDD = accountListRDD.withColumn(
      "account_list",
      F.concat(lit("["), F.concat_ws(",", col("account_list")), lit("]"))
    )

    transferAccountListRDD
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/account_in_out_list")

    val transferInTimeRDD = transferRDD.select(col("toId"), col("createTime"))
    val withdrawInTimeRDD = withdrawRDD.select(col("toId"), col("createTime"))
    val personGuaranteeTimeRDD =
      personGuaranteeRDD.select(col("fromId"), col("createTime"))
    val InvestInTimeRDD =
      personInvestRDD.select(col("investorId"), col("createTime"))
    val transferOutTimeRDD =
      transferRDD.select(col("fromId"), col("createTime"))
    val transactionsRDD = transferOutTimeRDD
      .union(withdrawRDD.select(col("fromId"), col("createTime")))

    val transferInOutPivotRDD =
      processByMonth(allAccounts, "account_id", "createTime", "account_id")

    transferInOutPivotRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/account_in_out_month")

    val transferInPivotRDD =
      processByMonth(transferInTimeRDD, "toId", "createTime", "account_id")

    transferInPivotRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/transfer_in_month")

    val withdrawInPivotRDD =
      processByMonth(withdrawInTimeRDD, "toId", "createTime", "account_id")

    withdrawInPivotRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/withdraw_in_month")

    val personGuaranteePivotRDD = processByMonth(
      personGuaranteeTimeRDD,
      "fromId",
      "createTime",
      "person_id"
    )

    personGuaranteePivotRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/person_guarantee_month")

    val InvestInPivotRDD =
      processByMonth(InvestInTimeRDD, "investorId", "createTime", "person_id")

    InvestInPivotRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/invest_month")

    val transferOutPivotRDD =
      processByMonth(transferOutTimeRDD, "fromId", "createTime", "account_id")

    transferOutPivotRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/transfer_out_month")

    val pivotRDD =
      processByMonth(transactionsRDD, "fromId", "createTime", "account_id")

    pivotRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/trans_withdraw_month")

    val withdrawInRDD =
      withdrawRDD.select(
        col("fromId"),
        col("toId"),
        col("amount").cast("double"),
        col("createTime")
      )
    val combinedRDD = transferItRDD.union(withdrawInRDD)

    val transformedAccountItemsRDD =
      transformItems(combinedRDD, "fromId", "toId")
    val transformedWithdrawInItemsRDD =
      transformItems(withdrawInRDD, "toId", "fromId")

    transformedAccountItemsRDD
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/trans_withdraw_items")

    transformedWithdrawInItemsRDD
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/account_withdraw_in_items")

    val transferOutAmountRDD =
      transferItRDD.select(col("fromId"), col("amount").cast("double"))
    val transferInAmountRDD =
      transferItRDD.select(col("toId"), col("amount").cast("double"))

    val transactionsAmountRDD = transferOutAmountRDD
      .union(withdrawRDD.select(col("fromId"), col("amount").cast("double")))

    val withdrawInBucketAmountRDD =
      withdrawInRDD.select(col("toId"), col("amount").cast("double"))

    val bucketCountsRDD =
      bucketAndCount(transactionsAmountRDD, "fromId", "amount", "account_id")

    bucketCountsRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/trans_withdraw_bucket")

    val withdrawInBucketCountsRDD =
      bucketAndCount(withdrawInBucketAmountRDD, "toId", "amount", "account_id")

    withdrawInBucketCountsRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/withdraw_in_bucket")
    val transferInBucketCountsRDD =
      bucketAndCount(transferInAmountRDD, "toId", "amount", "account_id")

    transferInBucketCountsRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/transfer_in_bucket")

    val transferOutBucketCountsRDD =
      bucketAndCount(transferOutAmountRDD, "fromId", "amount", "account_id")

    transferOutBucketCountsRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/transfer_out_bucket")

    val PersonOwnAccountRDD = ownRDD
      .select(col("personId"), col("accountId"))
      .groupBy("personId")
      .agg(coalesce(collect_set("accountId"), array()).alias("account_list"))
      .select(
        col("personId").alias("person_id"),
        concat(lit("["), array_join(col("account_list"), ","), lit("]"))
          .alias("account_list")
      )

    PersonOwnAccountRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/person_account_list")

    val PersonGuaranteeListRDD = personGuaranteeRDD
      .select(col("fromId"), col("toId"))
      .groupBy("fromId")
      .agg(coalesce(collect_set("toId"), array()).alias("guaranteee_list"))

    val PersonGuaranteePersonRDD = PersonGuaranteeListRDD.withColumn(
      "guaranteee_list",
      F.concat(lit("["), F.concat_ws(",", col("guaranteee_list")), lit("]"))
    )

    val PersonGuaranteeCount = PersonGuaranteeListRDD
      .select(col("fromId"), F.size(col("guaranteee_list")).alias("sum"))

    PersonGuaranteePersonRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/person_guarantee_list")

    PersonGuaranteeCount.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/person_guarantee_count")

    val loanAccountListRDD = depositRDD
      .groupBy("loanId")
      .agg(coalesce(collect_set("accountId"), array()).alias("account_list"))
      .select(
        col("loanId").alias("loan_id"),
        concat(lit("["), array_join(col("account_list"), ","), lit("]"))
          .alias("account_list")
      )

    loanAccountListRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/loan_account_list")

    val transactionsSumRDD = transferInAmountRDD
      .union(withdrawRDD.select(col("toId"), col("amount").cast("double")))

    val amountSumRDD = transactionsSumRDD
      .groupBy("toId")
      .agg(sum("amount").alias("amount"))
      .withColumnRenamed("toId", "account_id")
      .select(
        col("account_id"),
        format_string("%.2f", col("amount")).alias("amount")
      )

    amountSumRDD.write
      .option("header", "true")
      .option("delimiter", "|")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .save(s"${args.outputDir}/factor_table/upstream_amount")
  }
}
