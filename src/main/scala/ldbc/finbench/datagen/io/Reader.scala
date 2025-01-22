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

trait Reader[T] {
  type Ret

  def read(self: T): Ret
  def exists(self: T): Boolean
}

object Reader {
  type Aux[T, R] = Reader[T] { type Ret = R }

  def apply[T, R](implicit r: Reader.Aux[T, R]): Reader.Aux[T, R] = implicitly[Reader.Aux[T, R]]

  trait ReaderOps[T] {
    type Ret
    def tcInstance: Reader.Aux[T, Ret]
    def self: T
    def read: Ret = tcInstance.read(self)
  }

  object ReaderOps {
    type Aux[T, R] = ReaderOps[T] { type Ret = R }
  }

  object ops {
    import scala.language.implicitConversions
    implicit def toReaderOps[T, R](target: T)(implicit tc: Reader.Aux[T, R]): ReaderOps.Aux[T, R] =
      new ReaderOps[T] {
        override type Ret = R
        override def tcInstance: Aux[T, R] = tc
        override def self: T               = target
      }
  }
}
