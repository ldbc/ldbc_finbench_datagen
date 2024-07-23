package ldbc.finbench.datagen.generation

import ldbc.finbench.datagen.config.{ConfigParser, DatagenConfiguration}
import ldbc.finbench.datagen.generation.DatagenContext
import ldbc.finbench.datagen.io.raw.{Csv, Parquet, RawSink}
import ldbc.finbench.datagen.util._
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import java.net.URI
import java.util.Properties
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

object GenerationStage extends DatagenStage with Logging {

  case class Args(
      scaleFactor: String = "0.1",
      scaleFactorXml: String = "",
      params: Map[String, String] = Map.empty,
      paramFile: Option[String] = None,
      partitionsOpt: Option[Int] = None,
      outputDir: String = "out",
      numPartitions: Option[Int] = None,
      format: String = "csv"
  )

  override type ArgsType = Args

  override def run(args: Args): Unit = {
    // build and initialize the configs
    val config = buildConfig(args)
    // OPT: It is called in each SparkGenerator in Spark to initialize the context on the executors.
    // 1. Make the context as an object instead of a static class
    // 2. Pass the context to SparkContext instead of
    DatagenContext.initialize(config)

    log.info(
      s"Starting Finbench data generation of scale factor ${args.scaleFactor} to directory ${args.outputDir}."
    )

    // check the output format
    val format = args.format match {
      case "csv"     => Csv
      case "parquet" => Parquet
      case a =>
        throw new IllegalArgumentException(
          s"Format `${a}` is not supported by the generator."
        )
    }

    // TODO: It's better to define multiple job groups.
    SparkUI.job(
      implicitly[ClassTag[ActivitySimulator]].runtimeClass.getSimpleName,
      "serialize Finbench data"
    ) {
      val simulator = new ActivitySimulator(
        RawSink(args.outputDir, format, args.partitionsOpt)
      )
      simulator.simulate(config)
    }
  }

  private def buildConfig(args: Args): DatagenConfiguration = {
    val conf = new java.util.HashMap[String, String]
    val props = new Properties() // Read default values at first
    props.load(
      getClass.getClassLoader.getResourceAsStream("params_default.ini")
    )
    conf.putAll(props.asScala.asJava)

    for { paramsFile <- args.paramFile } conf.putAll(
      ConfigParser.readConfig(openPropFileStream(URI.create(paramsFile)))
    )

    for { (k, v) <- args.params } conf.put(k, v)

    for { partitions <- args.numPartitions } conf.put(
      "spark.partitions",
      partitions.toString
    ) // Following params will overwrite the values in params_default
    conf.putAll(
      ConfigParser.scaleFactorConf(args.scaleFactorXml, args.scaleFactor)
    ) // put scale factor conf
    conf.put("generator.outputDir", args.outputDir)
    conf.put("generator.format", args.format)

    new DatagenConfiguration(conf)
  }

  private def openPropFileStream(uri: URI): FSDataInputStream = {
    val fs = FileSystem.get(uri, spark.sparkContext.hadoopConfiguration)
    fs.open(new Path(uri.getPath))
  }
}
