package ldbc.finbench.datagen.factors

import ldbc.finbench.datagen.model.{Graph, GraphDef, Mode}
import org.apache.spark.sql.DataFrame

case class FactorTable[M <: Mode](
    name: String,
    data: DataFrame,
    source: Graph[M]
)

case class FactorTableDef[M <: Mode](
    name: String,
    sourceDef: GraphDef[M]
)
