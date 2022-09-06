package ldbc.finbench.datagen.generator

import java.net.URI
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

    // todo generate entities

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
    val conf = new java.util.HashMap[String, String]
    conf.putAll(ConfigParser.readConfig(getClass.getResourceAsStream("/params_default.ini")))

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
