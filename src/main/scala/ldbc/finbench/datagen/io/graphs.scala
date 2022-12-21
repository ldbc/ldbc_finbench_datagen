package ldbc.finbench.datagen.io

import ldbc.finbench.datagen.model.Mode.Raw.Layout
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.internal.Mode

object graphs {

  case class GraphSink(
      path: String,
      format: String,
      formatOptions: Map[String, String] = Map.empty,
      saveMode: SaveMode = SaveMode.ErrorIfExists
  )

  case class GraphSource[M <: Mode](implicit spark: SparkSession, en: DataFrame =:= Layout)
      extends Reader[GraphSource[M]] {
    @transient lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

    override type Ret = this.type

    override def read(self: GraphSource[M]): GraphSource.this.type = ???

    override def exists(self: GraphSource[M]): Boolean = ???
  }

}
