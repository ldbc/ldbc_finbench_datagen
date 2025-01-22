/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
