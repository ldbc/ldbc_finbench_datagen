package ldbc.finbench.datagen.io

import org.apache.spark.sql.SaveMode

package object raw {

  sealed trait RawFormat
  case object Csv     extends RawFormat { override def toString = "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat"     }
  case object Parquet extends RawFormat { override def toString = "parquet" }

  case class RawSink(
      outputDir: String,
      format: RawFormat,
      partitions: Option[Int] = None,
      formatOptions: Map[String, String] = Map.empty,
      mode: SaveMode = SaveMode.ErrorIfExists,
      partitionBy: Seq[String] = Seq.empty
  )
}
