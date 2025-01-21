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
