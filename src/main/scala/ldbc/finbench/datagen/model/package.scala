package ldbc.finbench.datagen

import ldbc.finbench.datagen.util.pascalToCamel

package object model {

  sealed trait Cardinality

  object Cardinality {
    case object OneN extends Cardinality
    case object NN   extends Cardinality
    case object NOne extends Cardinality
  }

  sealed trait EntityType {
    val entityPath: String
    val primaryKey: Seq[String]
  }

  object EntityType {
    final case class Node(name: String) extends EntityType {
      override val entityPath: String      = s"$name"
      override val primaryKey: Seq[String] = Seq("id")
      override def toString: String        = s"$name"
    }

    final case class Edge(`type`: String,
                          source: Node,
                          destination: Node,
                          cardinality: Cardinality,
                          sourceNameOverride: Option[String] = None,
                          destinationNameOverride: Option[String] = None)
        extends EntityType {
      val sourceName: String      = sourceNameOverride.getOrElse(source.name)
      val destinationName: String = destinationNameOverride.getOrElse(destination.name)

      override val entityPath: String = s"${sourceName}_${pascalToCamel(`type`)}_${destinationName}"

      override val primaryKey: Seq[String] =
        if (sourceName.equalsIgnoreCase(destinationName))
          Seq(s"${sourceName}1", s"${destinationName}2")
        else Seq(sourceName, destinationName)

      override def toString: String = s"${sourceName} - [s${`type`}] -> ${destinationName}"
    }
  }
}
