package ldbc.finbench.datagen

import ldbc.finbench.datagen.config.{ConfigParser, DatagenConfiguration}
import ldbc.finbench.datagen.factors.FactorGenerationStage
import ldbc.finbench.datagen.generation.dictionary.Dictionaries
import ldbc.finbench.datagen.generation.{DatagenContext, GenerationStage}
import ldbc.finbench.datagen.transformation.TransformationStage
import ldbc.finbench.datagen.util.SparkApp
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import shapeless.lens

import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters._

object LdbcDatagen extends SparkApp {
  val appName = "LDBC FinBench Datagen for Spark"

  case class Args(
     scaleFactor: String = "0.01",
     scaleFactorXml: String = "",
     params: Map[String, String] = Map.empty,
     paramFile: Option[String] = None,
     outputDir: String = "out",
     bulkloadPortion: Double = 0.97,
     keepImplicitDeletes: Boolean = false,
     batchPeriod: String = "day",
     numPartitions: Option[Int] = None,
     irFormat: String = "csv",
     format: String = "csv",
     formatOptions: Map[String, String] = Map.empty,
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

      opt[String]("scale-factor-xml")
        .valueName("scale-factor-xml")
        .action((x, c) => args.scaleFactorXml.set(c)(x))
        .text("The generator scale factor config xml")

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

      opt[Int]('n', "num-partitions")
        .action((x, c) => args.numPartitions.set(c)(Some(x)))
        .text("Controls parallelization and number of files written.")

      opt[Double]("bulkload-portion")
        .action((x, c) => args.bulkloadPortion.set(c)(x))
        .text("Bulkload portion. Only applicable to BI and interactive modes")

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

    val parsedArgs = parser.parse(args, Args()).getOrElse(throw new RuntimeException("Invalid arguments"))

    run(parsedArgs)
  }

  override def run(args: ArgsType): Unit = {
    // build and initialize the configs
    val config = buildConfig(args)
    DatagenContext.initialize(config)

    val generationArgs = GenerationStage.Args(
      scaleFactor = args.scaleFactor,
      outputDir = args.outputDir,
      format = args.format,
      partitionsOpt = args.numPartitions
    )
    GenerationStage.run(generationArgs)

//    if (args.generateFactors) {
//      val factorArgs = FactorGenerationStage.Args(
//        outputDir = args.outputDir,
//        format = args.factorFormat
//      )
//      FactorGenerationStage.run(factorArgs)
//    }
//
//    val transformArgs = TransformationStage.Args(
//      outputDir = args.outputDir,
//      keepImplicitDeletes = args.keepImplicitDeletes,
//      simulationStart = Dictionaries.dates.getSimulationStart,
//      simulationEnd = Dictionaries.dates.getSimulationEnd,
//      irFormat = args.irFormat,
//      format = args.format,
//      formatOptions = args.formatOptions,
//      epochMillis = args.epochMillis
//    )
//    TransformationStage.run(transformArgs)
  }


  private def buildConfig(args: Args): DatagenConfiguration = {
    val conf = new java.util.HashMap[String, String]
    val props = new Properties() // Read default values at first
    props.load(getClass.getClassLoader.getResourceAsStream("params_default.ini"))
    conf.putAll(props.asScala.asJava)

    for {paramsFile <- args.paramFile} conf.putAll(ConfigParser.readConfig(openPropFileStream(URI.create(paramsFile))))

    for {(k, v) <- args.params} conf.put(k, v)

    for {partitions <- args.numPartitions} conf.put("spark.partitions", partitions.toString) // Following params will overwrite the values in params_default
    conf.putAll(ConfigParser.scaleFactorConf(args.scaleFactorXml, args.scaleFactor)) // put scale factor conf
    conf.put("generator.outputDir", args.outputDir)
    conf.put("generator.format", args.format)

    new DatagenConfiguration(conf)
  }

  private def openPropFileStream(uri: URI): FSDataInputStream = {
    val fs = FileSystem.get(uri, spark.sparkContext.hadoopConfiguration)
    fs.open(new Path(uri.getPath))
  }
}
