package ldbc.finbench.datagen.generator

import java.net.URI
import java.util.Properties
import ldbc.finbench.datagen.generator.generators.SparkPersonGenerator
import ldbc.finbench.datagen.generator.serializers.RawSerializer
import ldbc.finbench.datagen.util.{
  ConfigParser,
  DatagenStage,
  GeneratorConfiguration,
  Logging,
  SparkUI
}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object GenerationStage extends DatagenStage with Logging {

  case class Args(scaleFactor: String = "",
                  numThreads: Option[Int] = None,
                  params: Map[String, String] = Map.empty,
                  paramFile: Option[String] = None,
                  outputDir: String = "out",
                  format: String = "csv",
                  oversizeFactor: Option[Double] = None)

  override type ArgsType = Args

  override def run(args: Args): Unit = {
    // build and initialize the configs
    val config = buildConfig(args)
    DatagenContext.initialize(config)

    // todo compute parallelism
    val personNum = 100000 // TODO use DatagenParams.numPerson
    val blockSize = 5000   // TODO use DatagenParams.blockSize
    val partitions = Math
      .min(Math.ceil(personNum.toDouble / blockSize).toLong, spark.sparkContext.defaultParallelism)
      .toInt

    // todo generate entities
    val persons = SparkPersonGenerator(config, Some(partitions))

    SparkUI.job(implicitly[ClassTag[RawSerializer]].runtimeClass.getSimpleName,
                "serialize Finbench data") {
      val rawSerializer = new RawSerializer
      rawSerializer.write()
    }
  }

  /**
    * build GeneratorConfiguration from user args
    *
    * @param args user params
    * @return {@link GeneratorConfiguration}
    */
  private def buildConfig(args: Args): GeneratorConfiguration = {
    val conf        = new java.util.HashMap[String, String]
    val props       = new Properties()
    val inputStream = getClass.getClassLoader.getResourceAsStream("params_default.ini")
    props.load(inputStream)
    conf.putAll(props.asScala.asJava)

    for { paramsFile <- args.paramFile } conf.putAll(
      ConfigParser.readConfig(openPropFileStream(URI.create(paramsFile))))

    for { (k, v) <- args.params } conf.put(k, v)

    for { numThreads <- args.numThreads } conf.put("hadoop.numThreads", numThreads.toString)

    // put scale factor conf
    val factorMap: Map[String, String] = Map()
    conf.putAll(factorMap.asJava)

    conf.put("generator.outputDir", args.outputDir)
    new GeneratorConfiguration(conf)
  }

  /**
    * read hdfs uri to get FSDataInputStream
    */
  private def openPropFileStream(uri: URI): FSDataInputStream = {
    val fs = FileSystem.get(uri, spark.sparkContext.hadoopConfiguration)
    fs.open(new Path(uri.getPath))
  }
}
