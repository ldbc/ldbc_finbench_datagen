package ldbc.finbench.datagen.transformation

import ldbc.finbench.datagen.model.Mode
import ldbc.finbench.datagen.util.DatagenStage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime}
import scopt.OptionParser
import shapeless.lens

object TransformationStage extends DatagenStage {

  case class Args(
      outputDir: String = "out",
      keepImplicitDeletes: Boolean = false,
      simulationStart: Long = 0,
      simulationEnd: Long = 0,
      irFormat: String = "csv",
      format: String = "csv",
      formatOptions: Map[String, String] = Map.empty,
      epochMillis: Boolean = false
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
    readSource
  }

  private def readSource(implicit spark: SparkSession) = {
    val transferDf = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/transfer/part-00000-1e389087-a54f-45b3-8f39-5de008042f68-c000.csv")

    val resRow = transferDf.select("fromId", "toId", "multiplicityId", "createTime", "deleteTime", "amount", "isExplicitDeleted")
      .withColumn("createTime", from_unixtime(col("createTime") / 1000, "yyyy-MM-dd'T'HH:mm:ss.SSS+00:00"))
      .withColumn("deleteTime", from_unixtime(col("deleteTime") / 1000, "yyyy-MM-dd'T'HH:mm:ss.SSS+00:00"))
      //      .filter("isExplicitDeleted = 'true'")
      .orderBy("createTime", "deleteTime")

    resRow.toDF().coalesce(1).write
      .format("csv")
      .option("header", value = true)
      .option("delimiter", "|")
      .option("encoding", "UTF-8")
      .mode("overwrite")
      .save("./out1/transfer")

//    val accountDf = spark.read.format("csv")
//      .option("header", "true")
//      .option("delimiter", "|")
//      .load("./out/account/part-00000-4b0e57cb-23bb-447f-89f1-e7e71a4ee017-c000.csv")
//
//    transferDf.join(accountDf, transferDf("fromId") === accountDf("id"), "left")
//      .select()

  }
}
