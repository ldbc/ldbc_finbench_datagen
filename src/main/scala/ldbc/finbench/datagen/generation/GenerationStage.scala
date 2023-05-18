package ldbc.finbench.datagen.generation

import ldbc.finbench.datagen.generation.generators.{SparkCompanyGenerator, SparkMediumGenerator, SparkPersonGenerator}
import ldbc.finbench.datagen.io.raw.{Csv, Parquet, RawSink}
import ldbc.finbench.datagen.util._
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object GenerationStage extends DatagenStage with Logging {

  case class Args(scaleFactor: String = "0.1", partitionsOpt: Option[Int] = None, params: Map[String, String] = Map.empty, paramFile: Option[String] = None, outputDir: String = "out", format: String = "csv")

  override type ArgsType = Args

  override def run(args: Args): Unit = {
    // build and initialize the configs
    val config = buildConfig(args)
    DatagenContext.initialize(config)

    // check the output format
    val format = args.format match {
      case "csv" => Csv
      case "parquet" => Parquet
      case a => throw new IllegalArgumentException(s"Format `${a}` is not supported by the generator.")
    }

    val sparkPartitions = if (config.getPartition != null) {
      Some(config.getPartition.toInt)
    } else {
      None
    }

    SparkUI.job(implicitly[ClassTag[ActivitySimulator]].runtimeClass.getSimpleName, "serialize Finbench data") {
      val simulator = new ActivitySimulator(RawSink(config.getOutputDir, format, sparkPartitions))
      simulator.simulate()
    }
  }

  /**
   * build GeneratorConfiguration from user args
   *
   * @param args user params
   * @return {@link GeneratorConfiguration} */
  private def buildConfig(args: Args): GeneratorConfiguration = {
    val conf = new java.util.HashMap[String, String]
    val props = new Properties() // Read default values at first
    props.load(getClass.getClassLoader.getResourceAsStream("params_default.ini"))
    conf.putAll(props.asScala.asJava)

    for {paramsFile <- args.paramFile} conf.putAll(ConfigParser.readConfig(openPropFileStream(URI.create(paramsFile))))

    for {(k, v) <- args.params} conf.put(k, v)

    for {partitions <- args.partitionsOpt} conf.put("spark.partitions", partitions.toString) // Following params will overwrite the values in params_default
    conf.putAll(ConfigParser.scaleFactorConf(args.scaleFactor)) // put scale factor conf
    conf.put("generator.outputDir", args.outputDir)
    conf.put("generator.format", args.format)

    new GeneratorConfiguration(conf)
  }

  /**
   * read hdfs uri to get FSDataInputStream */
  private def openPropFileStream(uri: URI): FSDataInputStream = {
    val fs = FileSystem.get(uri, spark.sparkContext.hadoopConfiguration)
    fs.open(new Path(uri.getPath))
  }
}
