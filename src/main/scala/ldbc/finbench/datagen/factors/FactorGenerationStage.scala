package ldbc.finbench.datagen.factors

import ldbc.finbench.datagen.util.DatagenStage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, countDistinct, row_number, var_pop}
import org.graphframes.GraphFrame
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

    val accountDf = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load("./out/account/*.csv")
    //      .toDF("_id","createTime","deleteTime","isBlocked","type","inDegree","OutDegree","isExplicitDeleted","Owner")

    val transferDf = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .csv("./out/transfer/*.csv")
      .toDF("src", "dst", "multiplicityI", "createTime", "deleteTime", "amount", "isExplicitDeleted")

    val graph = GraphFrame(accountDf, transferDf)

    val accountTransferAccountOut1Hops = graph.find("(a)-[e1]->(b)")
      .filter("e1.src == a.id")
      .groupBy("a.id")
      .agg(countDistinct("b.id").alias("AccountTransferAccountOut1Hops"))

    val accountTransferAccountOut2Hops = graph.find("(a)-[e1]->(b); (b)-[e2]->(c)")
      .filter("e1.src == a.id and e2.src == b.id")
      .groupBy("a.id")
      .agg(countDistinct("c.id").alias("AccountTransferAccountOut2Hops"))

    val accountTransferAccountOut3Hops = graph.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(d)")
      .filter("e1.src == a.id and e2.src == b.id and e3.src == c.id")
      .groupBy("a.id")
      .agg(countDistinct("d.id").alias("AccountTransferAccountOut3Hops"))

    val factorTable = accountTransferAccountOut1Hops
      .join(accountTransferAccountOut2Hops.join(accountTransferAccountOut3Hops, "id"), "id")

    val w1 = Window.orderBy(col("AccountTransferAccountOut1Hops")).rowsBetween(-100000, 0)
    val variance1 = var_pop(col("AccountTransferAccountOut1Hops")).over(w1)
    val df1 = accountTransferAccountOut1Hops
      .withColumn("variance1", variance1)
      .filter(col("AccountTransferAccountOut1Hops") > 1)
      .withColumn("row_number1", row_number.over(Window.orderBy("variance1")))
      .filter(col("row_number1") <= 100000)


    val w2 = Window.orderBy(col("AccountTransferAccountOut2Hops")).rowsBetween(-10000, 0)
    val variance2 = var_pop(col("AccountTransferAccountOut2Hops")).over(w2)
    val df2 = df1
      .join(accountTransferAccountOut2Hops, "id")
      .withColumn("variance2", variance2)
      .filter(col("AccountTransferAccountOut2Hops") > 10)
      .withColumn("row_number2", row_number.over(Window.orderBy("variance2")))
      .filter(col("row_number2") <= 10000)

    val w3 = Window.orderBy(col("AccountTransferAccountOut3Hops")).rowsBetween(-1000, 0)
    val variance3 = var_pop(col("AccountTransferAccountOut3Hops")).over(w3)
    val paramTable = df2
      .join(accountTransferAccountOut3Hops, "id")
      .withColumn("variance3", variance3)
      .filter(col("AccountTransferAccountOut3Hops") > 100)
      .withColumn("row_number3", row_number.over(Window.orderBy("variance3")))
      .filter(col("row_number3") <= 1000)
      .select("id", "AccountTransferAccountOut1Hops", "AccountTransferAccountOut2Hops", "AccountTransferAccountOut3Hops")

    factorTable.coalesce(1).write
      .format("csv")
      .option("header", value = true)
      .option("delimiter", "|")
      .option("encoding", "UTF-8")
      .mode("overwrite")
      .save("./out/factorTable/")

    paramTable.coalesce(1).write
      .format("csv")
      .option("header", value = true)
      .option("delimiter", "|")
      .option("encoding", "UTF-8")
      .mode("overwrite")
      .save("./out/paramTable/")

  }
}
