package ldbc.finbench.datagen.transformation

import ldbc.finbench.datagen.io.graphs.{GraphSink, GraphSource}
import ldbc.finbench.datagen.model
import ldbc.finbench.datagen.model.{BatchedEntity, Graph, Mode}
import ldbc.finbench.datagen.util.DatagenStage
import org.apache.spark.sql.DataFrame
import shapeless.Poly1

object TransformationStage extends DatagenStage {

  case class Args(
      outputDir: String = "out",
      explodeEdges: Boolean = false,
      explodeAttrs: Boolean = false,
      keepImplicitDeletes: Boolean = false,
      simulationStart: Long = 0,
      simulationEnd: Long = 0,
      mode: Mode = Mode.Raw,
      irFormat: String = "parquet",
      format: String = "csv",
      formatOptions: Map[String, String] = Map.empty,
      epochMillis: Boolean = false
  )

  override type ArgsType = Args

  /**
    * execute the transform process
    */
  override def run(args: Args): Unit = {}
}
