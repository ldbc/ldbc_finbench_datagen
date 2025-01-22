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

package ldbc.finbench.datagen.syntax

import org.apache.spark.sql.{Column, ColumnName, DataFrame, Dataset}

import scala.language.implicitConversions

trait SparkSqlSyntax {
  @`inline` implicit final def datasetOps[A](a: Dataset[A])           = new DatasetOps(a)
  @`inline` implicit final def stringToColumnOps[A](a: StringContext) = new StringToColumnOps(a)
}

final class DatasetOps[A](private val self: Dataset[A]) extends AnyVal {
  def |+|(other: Dataset[A]): Dataset[A] = self union other

  def select(columns: Seq[Column]): DataFrame = self.select(columns: _*)

  def partition(expr: Column): (Dataset[A], Dataset[A]) = {
    val df = self.cache()
    (df.filter(expr), df.filter(!expr || expr.isNull))
  }
}

final class StringToColumnOps(private val sc: StringContext) extends AnyVal {
  def $(args: Any*): ColumnName = new ColumnName(sc.s(args: _*))
}
