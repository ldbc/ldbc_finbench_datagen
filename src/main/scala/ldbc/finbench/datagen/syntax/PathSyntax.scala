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

import org.apache.hadoop.fs.Path

import java.net.URI
import scala.language.implicitConversions

trait PathSyntax {
  @`inline` implicit final def pathSyntaxOpsForString[A](a: String): PathSyntaxOpsForString = new PathSyntaxOpsForString(a)
  @`inline` implicit final def pathSyntaxOpsForPath[A](a: Path): PathSyntaxOpsForPath       = new PathSyntaxOpsForPath(a)
  @`inline` implicit final def pathSyntaxOpsForUri[A](a: URI): PathSyntaxOpsForUri          = new PathSyntaxOpsForUri(a)
}

final class PathSyntaxOpsForString(private val self: String) extends AnyVal {
  import PathSyntaxOpsHelpers._
  def /(child: String): Path = join(new Path(self), new Path(child))
  def /(child: Path): Path   = join(new Path(self), child)
}

final class PathSyntaxOpsForPath(private val self: Path) extends AnyVal {
  import PathSyntaxOpsHelpers._
  def /(child: String): Path = join(self, new Path(child))
  def /(child: Path): Path   = join(self, child)
}

final class PathSyntaxOpsForUri(private val self: URI) extends AnyVal {
  import PathSyntaxOpsHelpers._
  def /(child: String): Path = join(new Path(self), new Path(child))
  def /(child: Path): Path   = join(new Path(self), child)
}

private[syntax] object PathSyntaxOpsHelpers {
  def join(path1: Path, path2: Path): Path = new Path(ensureTrailingSlashForAbsoluteUri(path1), path2)

  private[this] def ensureTrailingSlashForAbsoluteUri(path: Path): Path = {
    if (path.isAbsolute)
      return path

    val uri = path.toUri

    if (uri.getScheme == null || uri.getPath != "")
      return path

    new Path(new URI(uri.getScheme, uri.getAuthority, "/", uri.getQuery, uri.getFragment))
  }
}
