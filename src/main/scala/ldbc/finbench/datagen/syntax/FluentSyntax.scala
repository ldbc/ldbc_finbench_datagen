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

import scala.language.implicitConversions

trait FluentSyntax {
  @`inline` implicit final def fluentSyntaxOps[A](a: A) = new FluentSyntaxOps(a)
}

final class FluentSyntaxOps[A](private val self: A) extends AnyVal {

  /** Fluent syntax for folding with self as the base item.
    */
  def pipeFoldLeft[F](foldable: TraversableOnce[F])(op: (A, F) => A): A = {
    foldable.foldLeft(self)(op)
  }

  /** Fluent syntax for applying a function on self. d
    */
  def pipe[R](f: A => R): R = f(self)

  /** Fluent syntax for applying a side-effect on self.
    */
  def tap(f: A => Unit): A = {
    f(self)
    self
  }
}
