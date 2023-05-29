package ldbc.finbench.datagen.generation

import ldbc.finbench.datagen.io.raw.{Csv, Parquet, RawSink}
import ldbc.finbench.datagen.util._

import scala.reflect.ClassTag

object GenerationStage extends DatagenStage with Logging {

  case class Args(scaleFactor: String = "0.1", partitionsOpt: Option[Int] = None, outputDir: String = "out", format: String = "csv")

  override type ArgsType = Args

  override def run(args: Args): Unit = {
    log.info(s"Starting Finbench data generation of scale factor ${args.scaleFactor} to directory ${args.outputDir}.")

    // check the output format
    val format = args.format match {
      case "csv" => Csv
      case "parquet" => Parquet
      case a => throw new IllegalArgumentException(s"Format `${a}` is not supported by the generator.")
    }

    // TODO: It's better to define multiple job groups.
    SparkUI.job(implicitly[ClassTag[ActivitySimulator]].runtimeClass.getSimpleName, "serialize Finbench data") {
      val simulator = new ActivitySimulator(RawSink(args.outputDir, format, args.partitionsOpt))
      simulator.simulate()
    }
  }
}
