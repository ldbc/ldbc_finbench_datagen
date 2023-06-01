package ldbc.finbench.datagen.transformation

import ldbc.finbench.datagen.generation.DatagenParams
import ldbc.finbench.datagen.generation.dictionary.Dictionaries
import ldbc.finbench.datagen.util.sql.qcol
import ldbc.finbench.datagen.util.{DatagenStage, Logging}
import ldbc.finbench.datagen.syntax._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, date_trunc, from_unixtime, lit, to_timestamp}
import scopt.OptionParser
import shapeless.lens

// Note: transformation is not used now. Data conversion is done by python scripts.
object TransformationStage extends DatagenStage with Logging {
  private val options: Map[String, String] = Map("header" -> "true", "delimiter" -> "|")

  case class Args(
                   outputDir: String = "out",
                   bulkloadPortion: Double = 0.0,
                   keepImplicitDeletes: Boolean = false,
                   simulationStart: Long = 0,
                   simulationEnd: Long = 0,
                   irFormat: String = "csv",
                   format: String = "csv",
                   formatOptions: Map[String, String] = Map.empty,
                   epochMillis: Boolean = false,
                   batchPeriod: String = "day"
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

      help('h', "help").text("prints this usage text")
    }
    val parsedArgs =
      parser.parse(args, Args()).getOrElse(throw new RuntimeException("Invalid arguments"))

    run(parsedArgs)
  }

  // execute the transform process
  override def run(args: Args): Unit = {

    val rawPathPrefix = args.outputDir / "raw"
    val outputPathPrefix = args.outputDir / "history_data"

    val filterDeletion = false

    val simulationStart = Dictionaries.dates.getSimulationStart
    val simulationEnd = Dictionaries.dates.getSimulationEnd
    val bulkLoadThreshold = calculateBulkLoadThreshold(args.bulkloadPortion, simulationStart, simulationEnd)

    //    val batch_id = (col: Column) => date_format(date_trunc(args.batchPeriod, to_timestamp(col / lit(1000L))), batchPeriodFormat(args.batchPeriod))
    //
    //    def inBatch(col: Column, batchStart: Long, batchEnd: Long) =
    //      col >= lit(batchStart) && col < lit(batchEnd)
    //
    //    val batched = (df: DataFrame) =>
    //      df
    //        .select(
    //          df.columns.map(qcol) ++ Seq(
    //            batch_id($"creationDate").as("insert_batch_id"),
    //            batch_id($"deletionDate").as("delete_batch_id")
    //          ): _*
    //        )
    //
    //    val insertBatchPart = (tpe: EntityType, df: DataFrame, batchStart: Long, batchEnd: Long) => {
    //      df
    //        .filter(inBatch($"creationDate", batchStart, batchEnd))
    //        .pipe(batched)
    //        .select(
    //          Seq($"insert_batch_id".as("batch_id")) ++ columns(tpe, df.columns).map(qcol): _*
    //        )
    //    }
    //
    //    val deleteBatchPart = (tpe: EntityType, df: DataFrame, batchStart: Long, batchEnd: Long) => {
    //      val idColumns = tpe.primaryKey.map(qcol)
    //      df
    //        .filter(inBatch($"deletionDate", batchStart, batchEnd))
    //        .filter(if (df.columns.contains("explicitlyDeleted")) col("explicitlyDeleted") else lit(true))
    //        .pipe(batched)
    //        .select(Seq($"delete_batch_id".as("batch_id"), $"deletionDate") ++ idColumns: _*)
    //    }

    val readRaw = (target: String) => {
      spark.read.format(args.irFormat)
        .options(options)
        .option("inferSchema", "true")
        .load(s"$rawPathPrefix/$target/*.csv")
    }


    val extractSnapshot = (df: DataFrame) => {
      df.filter($"creationDate" < lit(bulkLoadThreshold)
        && (!lit(filterDeletion) || $"deletionDate" >= lit(bulkLoadThreshold)))
      //        .select(_: _*)
    }

    val transferSnapshot = extractSnapshot(readRaw("transfer"))
      //      .select("fromId", "toId", "multiplicityId", "createTime", "deleteTime", "amount", "isExplicitDeleted")
      //      .map(extractSnapshot)
      .withColumn("createTime", from_unixtime(col("createTime") / 1000, batchPeriodFormat(args.batchPeriod)))
      .withColumn("deleteTime", from_unixtime(col("deleteTime") / 1000, batchPeriodFormat(args.batchPeriod)))
      .orderBy("createTime", "deleteTime")
    write(transferSnapshot, (outputPathPrefix / "transfer").toString)

    //    val accountDf = spark.read.format("csv")
    //      .option("header", "true")
    //      .option("delimiter", "|")
    //      .load("./out/account/part-00000-4b0e57cb-23bb-447f-89f1-e7e71a4ee017-c000.csv")
    //
    //    transferDf.join(accountDf, transferDf("fromId") === accountDf("id"), "left")
    //      .select()
  }

  //  def columns(tpe: EntityType, cols: Seq[String]) = tpe match {
  //    case tpe if tpe.isStatic => cols
  //    case Edge("Knows", PersonType, PersonType, NN, false, _, _) =>
  //      val rawCols = Set("deletionDate", "explicitlyDeleted", "weight")
  //      cols.filter(!rawCols.contains(_))
  //    case _ =>
  //      val rawCols = Set("deletionDate", "explicitlyDeleted")
  //      cols.filter(!rawCols.contains(_))
  //  }
  private def write(data: DataFrame, path: String): Unit = {
    data.toDF().coalesce(1)
      .write.format("csv").options(options).option("encoding", "UTF-8")
      .mode("overwrite").save(path)
  }

  private def calculateBulkLoadThreshold(bulkLoadPortion: Double, simulationStart: Long, simulationEnd: Long) = {
    (simulationEnd - ((simulationEnd - simulationStart) * (1 - bulkLoadPortion)).toLong)
  }

  private def batchPeriodFormat(batchPeriod: String) = batchPeriod match {
    case "year" => "yyyy"
    case "month" => "yyyy-MM"
    case "day" => "yyyy-MM-dd"
    case "hour" => "yyyy-MM-dd'T'hh"
    case "minute" => "yyyy-MM-dd'T'hh:mm"
    case "second" => "yyyy-MM-dd'T'hh:mm:ss"
    case "millisecond" => "yyyy-MM-dd'T'hh:mm:ss.SSS"
    case _ => throw new IllegalArgumentException("Unrecognized partition key")
  }
}
