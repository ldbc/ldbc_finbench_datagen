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

trait Writer[S] {
  type Data
//  def write(self: Data, sink: S): Unit
}

object Writer {
  type Aux[S, D] = Writer[S] { type Data = D }
  def apply[S, D](implicit r: Writer.Aux[S, D]): Writer.Aux[S, D] = implicitly[Writer.Aux[S, D]]

  trait WriterOps[Data] {
    type Sink
    def tcInstance: Writer.Aux[Sink, Data]
    def self: Data
  }

  object WriterOps {
    type Aux[Data, S] = WriterOps[Data] { type Sink = S }
  }

  object ops {
    import scala.language.implicitConversions
    implicit def toWriterOps[Data, S](target: Data)(
        implicit tc: Writer.Aux[S, Data]): WriterOps.Aux[Data, S] = new WriterOps[Data] {
      override type Sink = S
      override def tcInstance: Aux[S, Data] = tc
      override def self: Data               = target
    }
  }

}
