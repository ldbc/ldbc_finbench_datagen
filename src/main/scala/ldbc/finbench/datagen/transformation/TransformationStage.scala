//package ldbc.finbench.datagen.transformation
//
//import ldbc.finbench.datagen.io.graphs.GraphSource
//import ldbc.finbench.datagen.model.{Graph, Mode}
//import ldbc.finbench.datagen.util.DatagenStage
//import org.apache.spark.sql.DataFrame
//import shapeless.Poly1
//
//object TransformationStage extends DatagenStage {
//  case class Args(
//      outputDir: String = "out",
//      explodeEdges: Boolean = false,
//      explodeAttrs: Boolean = false,
//      keepImplicitDeletes: Boolean = false,
//      simulationStart: Long = 0,
//      simulationEnd: Long = 0,
//      mode: Mode = Mode.Raw,
//      irFormat: String = "parquet",
//      format: String = "csv",
//      formatOptions: Map[String, String] = Map.empty,
//      epochMillis: Boolean = false
//  )
//
//  override type ArgsType = Args
//
//  /**
//    * execute the transform process
//    */
//  override def run(args: Args): Unit = {
//    object write extends Poly1 {
//      implicit def caseSimple[M <: Mode](implicit ev: M#Layout =:= DataFrame) =
//        at[Graph[M]](g => g.write(GraphSink(args.outputDir, args.format, args.formatOptions)))
//    }
//
//    implicit def caseBatched[M <: Mode](implicit ev: M#Layout =:= BatchedEntity) =
//      at[Graph[M]](g = g.write(GraphSink(args.outputDir, args.format, args.formatOptions)))
//  }
//
//  object convertDates extends Poly1 {
//    implicit def caseSimple[M <: Mode](implicit ev: DataFrame =:= M#Layout) =
//      at[Graph[M]](g => ConvertDates[M].transform(g))
//
//    implicit def caseBatched[M <: Mode](implicit en: BatchedEntity =:= M#Layout) =
//      at[Graph[M]](g => ConvertDates[M].transform(g))
//  }
//
//  type Out = Graph[Mode.Raw.type] :+: Graph[Mode.Interactive] :+: Graph[Mode.BI] :+: CNil
//
//  GraphSource(model.graphs.Raw.graphDef, args.outputDir, args.irFormat).read
//    .pipeFoldLeft(args.explodeAttrs.fork)((graph, _: Unit) => ExplodeAttrs.transform(graph))
//    .pipeFoldLeft(args.explodeEdges.fork)((graph, _: Unit) => ExplodeEdges.transform(graph))
//    .pipe[Out] { g =>
//      args.mode match {
//        case bi @ Mode.BI(_, _) => null
//        case interactive @ Mode.Interactive(_) =>
//          Coproduct[Out](
//            RawToInteractiveTransform(interactive, args.simulationStart, args.simulationEnd)
//              .transform(g))
//        case Mode.Raw => Coproduct[Out](g)
//      }
//    }
//    .pipeFoldLeft((!args.epochMillis).fork)((graph, _: Unit) => graph.map(convertDates))
//    .map(write)
//}
