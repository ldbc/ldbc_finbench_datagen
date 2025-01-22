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

package ldbc.finbench.datagen.util

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Column, ColumnName, Encoder, Encoders}

import scala.collection.Map
import scala.reflect.runtime.universe.TypeTag

object sql extends EncoderInstances {
  def qcol(name: String): Column = new ColumnName(qualified(name))

  def qualified(col: String) = s"`$col`"
}

// This was extracted from Spark so that we don't have to require a SparkSession needlessly
trait EncoderInstances extends LowPriorityEncoderInstances {
  // Scala Primitives
  implicit def newIntEncoder: Encoder[Int]         = Encoders.scalaInt
  implicit def newLongEncoder: Encoder[Long]       = Encoders.scalaLong
  implicit def newDoubleEncoder: Encoder[Double]   = Encoders.scalaDouble
  implicit def newFloatEncoder: Encoder[Float]     = Encoders.scalaFloat
  implicit def newByteEncoder: Encoder[Byte]       = Encoders.scalaByte
  implicit def newShortEncoder: Encoder[Short]     = Encoders.scalaShort
  implicit def newBooleanEncoder: Encoder[Boolean] = Encoders.scalaBoolean
  implicit def newStringEncoder: Encoder[String]   = Encoders.STRING

  // Java stuff
  implicit def newJavaDecimalEncoder: Encoder[java.math.BigDecimal]            = Encoders.DECIMAL
  implicit def newScalaDecimalEncoder: Encoder[scala.math.BigDecimal]          = ExpressionEncoder()
  implicit def newDateEncoder: Encoder[java.sql.Date]                          = Encoders.DATE
  implicit def newTimeStampEncoder: Encoder[java.sql.Timestamp]                = Encoders.TIMESTAMP
  implicit def newJavaEnumEncoder[A <: java.lang.Enum[_]: TypeTag]: Encoder[A] = ExpressionEncoder()
  implicit def newBoxedIntEncoder: Encoder[java.lang.Integer]                  = Encoders.INT
  implicit def newBoxedLongEncoder: Encoder[java.lang.Long]                    = Encoders.LONG
  implicit def newBoxedDoubleEncoder: Encoder[java.lang.Double]                = Encoders.DOUBLE
  implicit def newBoxedFloatEncoder: Encoder[java.lang.Float]                  = Encoders.FLOAT
  implicit def newBoxedByteEncoder: Encoder[java.lang.Byte]                    = Encoders.BYTE
  implicit def newBoxedShortEncoder: Encoder[java.lang.Short]                  = Encoders.SHORT
  implicit def newBoxedBooleanEncoder: Encoder[java.lang.Boolean]              = Encoders.BOOLEAN

  // Collections
  implicit def newSequenceEncoder[T <: Seq[_]: TypeTag]: Encoder[T] = ExpressionEncoder()
  implicit def newMapEncoder[T <: Map[_, _]: TypeTag]: Encoder[T]   = ExpressionEncoder()
  implicit def newSetEncoder[T <: Set[_]: TypeTag]: Encoder[T]      = ExpressionEncoder()

  // Arrays
  implicit def newIntArrayEncoder: Encoder[Array[Int]]                          = ExpressionEncoder()
  implicit def newLongArrayEncoder: Encoder[Array[Long]]                        = ExpressionEncoder()
  implicit def newDoubleArrayEncoder: Encoder[Array[Double]]                    = ExpressionEncoder()
  implicit def newFloatArrayEncoder: Encoder[Array[Float]]                      = ExpressionEncoder()
  implicit def newByteArrayEncoder: Encoder[Array[Byte]]                        = Encoders.BINARY
  implicit def newShortArrayEncoder: Encoder[Array[Short]]                      = ExpressionEncoder()
  implicit def newBooleanArrayEncoder: Encoder[Array[Boolean]]                  = ExpressionEncoder()
  implicit def newStringArrayEncoder: Encoder[Array[String]]                    = ExpressionEncoder()
  implicit def newProductArrayEncoder[A <: Product: TypeTag]: Encoder[Array[A]] = ExpressionEncoder()
}

trait LowPriorityEncoderInstances {
  implicit def newProductEncoder[T <: Product: TypeTag]: Encoder[T] = Encoders.product[T]
}
