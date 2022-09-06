package ldbc.finbench.datagen

import ldbc.finbench.datagen.generator.GenerationStage
import ldbc.finbench.datagen.util.SparkApp
import shapeless.lens

object LdbcDatagen extends SparkApp {
  val appName = "LDBC FinBench Datagen for Spark"
  case class Args(
      scaleFactor: String = "1",
      params: Map[String, String] = Map.empty,
      paramFile: Option[String] = None,
      outputDir: String = "out",
      bulkloadPortion: Double = 0.97,
      explodeEdges: Boolean = false,
      explodeAttrs: Boolean = false,
      keepImplicitDeletes: Boolean = false,
      mode: String = "raw",
      batchPeriod: String = "day",
      numThreads: Option[Int] = None,
      format: String = "csv",
      formatOptions: Map[String, String] = Map.empty,
      oversizeFactor: Option[Double] = None,
      epochMillis: Boolean = false,
      generateFactors: Boolean = false,
      factorFormat: String = "parquet"
  )

  override type ArgsType = Args

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Args](getClass.getName.dropRight(1)) {
      head(appName)

      val args = lens[ArgsType]

      opt[String]("scale-factor")
        .valueName("scale-factor")
        .action((x, c) => args.scaleFactor.set(c)(x))
        .text("The generator scale factor")

      opt[Map[String, String]]('p', "params")
        .action((x, c) => args.params.set(c)(x))
        .text("Key=value params passed to the generator. Takes precedence over --param-file")

      opt[String]('P', "param-file")
        .action((x, c) => args.paramFile.set(c)(Some(x)))
        .text("Parameter file used for the generator")

      opt[String]('o', "output-dir")
        .action((x, c) => args.outputDir.set(c)(x))
        .text(
          "path on the cluster filesystem, where Datagen outputs. Can be a URI (e.g S3, ADLS, HDFS) or a " +
            "path in which case the default cluster file system is used."
        )

      opt[Int]('n', "num-threads")
        .action((x, c) => args.numThreads.set(c)(Some(x)))
        .text("Controls parallelization and number of files written.")

      opt[String]('m', "mode")
        .validate(s =>
          s.toLowerCase match {
            case "raw" | "bi" | "interactive" => Right(())
            case _                            => Left("Invalid value. Must be one of raw, bi, interactive")
        })
        .action((x, c) => args.mode.set(c)(x))
        .text("Generation mode. Options: raw, bi, interactive. Default: raw")

      opt[Double]("oversize-factor")
        .action((x, c) => args.oversizeFactor.set(c)(Some(x)))
        .text(
          "Controls size of files relative to Persons. " +
            "Values larger than 1 will result in less but larger files. " +
            "Smaller values result in more, smaller files"
        )

      opt[Double]("bulkload-portion")
        .action((x, c) => args.bulkloadPortion.set(c)(x))
        .text("Bulkload portion. Only applicable to BI and interactive modes")

      opt[Unit]('e', "explode-edges")
        .action((x, c) => args.explodeEdges.set(c)(true))

      opt[Unit]('a', "explode-attrs")
        .action((x, c) => args.explodeAttrs.set(c)(true))

      opt[String]("batch-period")
        .action((x, c) => args.batchPeriod.set(c)(x))
        .text(
          "Period of the batches in BI mode. Possible values: year, month, day, hour. Default: day")

      opt[String]('f', "format")
        .action((x, c) => args.format.set(c)(x))
        .text("Output format. Currently, Spark Datasource formats are supported, such as 'csv', 'parquet' or 'orc'.")

      opt[Unit]("keep-implicit-deletes")
        .action((x, c) => args.keepImplicitDeletes.set(c)(true))
        .text(
          "Keep implicit deletes. Only applicable to BI mode. By default the BI output doesn't contain dynamic entities" +
            "without the explicitlyDeleted attribute and removes the rows where the attribute is false." +
            "Setting this flag retains all deletes."
        )

      opt[Map[String, String]]("format-options")
        .action((x, c) => args.formatOptions.set(c)(x))
        .text(
          "Output format options specified as key=value1[,key=value...]. See format options for specific formats " +
            "available in Spark: https://spark.apache.org/docs/2.4.5/api/scala/index.html#org.apache.spark.sql.DataFrameWriter"
        )

      opt[Unit]("generate-factors")
        .action((x, c) => args.generateFactors.set(c)(true))
        .text("Generate factor tables")

      opt[String]("factor-format")
        .action((x, c) => args.factorFormat.set(c)(x))
        .text("Output format of factor tables")

      help('h', "help").text("prints this usage text")

      opt[Unit]("epoch-millis")
        .action((x, c) => args.epochMillis.set(c)(true))
        .text("Use longs with millis since Unix epoch instead of native dates")
    }

    val parsedArgs =
      parser.parse(args, Args()).getOrElse(throw new RuntimeException("Invalid arguments"))

    run(parsedArgs)
  }

  override def run(args: ArgsType): Unit = {

    val generatorArgs = GenerationStage.Args(
      scaleFactor = args.scaleFactor,
      params = args.params,
      paramFile = args.paramFile,
      outputDir = args.outputDir,
      numThreads = args.numThreads,
      format = args.format,
      oversizeFactor = args.oversizeFactor
    )

    GenerationStage.run(generatorArgs)

    //val transformArgs = null
    //TransformationStage.run(transformArgs)
  }
}
