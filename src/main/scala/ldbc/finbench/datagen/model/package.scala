package ldbc.finbench.datagen

import ldbc.finbench.datagen.io.graphs
import ldbc.finbench.datagen.util.pascalToCamel
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Encoder}
import org.apache.spark.sql.types.StructType
import shapeless._

package object model {
  type Id[A] = A

  sealed trait Cardinality

  object Cardinality {
    case object OneN extends Cardinality
    case object NN   extends Cardinality
    case object NOne extends Cardinality
  }

  trait EntityTraits[A] {
    def `type`: EntityType
    def schema: StructType
    def sizeFactor: Double
  }

  object EntityTraits {
    def apply[A: EntityTraits] = implicitly[EntityTraits[A]]

    def pure[A: Encoder](`type`: EntityType, sizeFactor: Double): EntityTraits[A] = {
      val _type       = `type`
      val _sizeFactor = sizeFactor
      val _schema     = implicitly[Encoder[A]].schema
      new EntityTraits[A] {
        override def `type`: EntityType = _type
        override def schema: StructType = _schema
        override def sizeFactor: Double = _sizeFactor
      }
    }
  }

  sealed trait EntityType {
    val entityPath: String
    val primaryKey: Seq[String]
  }

  object EntityType {

    /**
      * define Node class
      */
    final case class Node(name: String) extends EntityType {
      override val entityPath: String      = s"$name"
      override val primaryKey: Seq[String] = Seq("id")
      override def toString: String        = s"$name"
    }

    /**
      * define Edge class
      */
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
        (if (sourceName.equalsIgnoreCase(destinationName))
           Seq(s"${sourceName}1", s"${destinationName}2")
         else Seq(sourceName, destinationName)).map(name => s"{$name}Id")

      override def toString: String = s"${sourceName} - [s${`type`}] -> ${destinationName}"
    }

    /**
      * define Attr class
      */
    final case class Attr(`type`: String, parent: Node, attribute: String) extends EntityType {

      override val entityPath: String = s"${parent.name}_${pascalToCamel(`type`)}_${attribute}"

      override val primaryKey: Seq[String] = {
        if (parent.name.equals(attribute))
          Seq(s"${parent.name}1", s"${attribute}2")
        else Seq(parent.name, attribute)
      }.map(name => s"${name}Id")

      override def toString: String = s"${parent.name} ♢-[${`type`}] -> $attribute"
    }
  }

  case class Batched(entity: DataFrame, batchId: Seq[String], ordering: Seq[Column])

  case class BatchedEntity(
      snapshot: DataFrame,
      insertBatches: Option[Batched],
      deleteBatches: Option[Batched]
  )

  sealed trait Mode {
    type Layout
    def modePath: String
  }

  object Mode {

    /**
      * Raw mode
      */
    final case object Raw extends Mode {
      type layout = DataFrame
      override val modePath: String = "raw"

      // append three columns based on the original columns
      def withRawColumns(entityType: EntityType, cols: Column*): Seq[Column] =
        Some().foldLeft(cols)(
          (cols, _) =>
            Seq(
              col("creationDate"),
              col("deletionDate"),
              col("explicitlyDeleted")
            ) ++ cols)

      def dateTimePattern = "yyyy-MM-dd'T'HH:mm:ss.SSS+00:00"
      def datePattern     = "yyyy-MM-dd"
    }

    final case object Interactive extends Mode {
      type Layout = DataFrame
      override val modePath: String = "interactive"
    }

    final case object BI extends Mode {
      type Layout = DataFrame
      override val modePath: String = "bi"
    }
  }

  trait GraphLike[+M <: Mode] {
    def isAttrExploded: Boolean
    def isEdgesExploded: Boolean
    def mode: M
  }

  case class Graph[+M <: Mode](
      isAttrExploded: Boolean,
      isEdgesExploded: Boolean,
      mode: M,
      entities: Map[EntityType, M#Layout]
  ) extends GraphLike[M]

  case class GraphDef[M <: Mode](
      isAttrExploded: Boolean,
      isEdgesExploded: Boolean,
      mode: M,
      entities: Map[EntityType, Option[String]]
  ) extends GraphLike[M]

  sealed trait BatchPeriod

  object BatchPeriod {
    case object Day   extends BatchPeriod
    case object Month extends BatchPeriod
  }

}
