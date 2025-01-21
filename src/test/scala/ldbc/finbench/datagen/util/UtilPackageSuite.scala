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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class UtilPackageSuite extends AnyFunSuite with BeforeAndAfterAll {

  test("simpleNameOf") {
    val simpleName = simpleNameOf[String]
    assert(simpleName.equals("String"))
  }

  test("pascalToCamel") {
    val actualResult = pascalToCamel("PersonInvestCompany")
    val expectResult = "personInvestCompany"
    assert(actualResult.equals(expectResult))

    val actualEmptyResult = pascalToCamel("")
    val expectEmptyResult = ""
    assert(actualEmptyResult.equals(expectEmptyResult))

    assertThrows[NullPointerException](pascalToCamel(null))
  }

  test("camelToUpper") {
    val actualResult = camelToUpper("hasTag")
    val expectResult = "HAS_TAG"
    assert(actualResult.equals(expectResult))

    val actualEmtpyResult = camelToUpper("")
    val expectEmptyResult = ""
    assert(actualEmtpyResult.equals(expectEmptyResult))

    assertThrows[NullPointerException](camelToUpper(null))
  }

  test("lower") {
    val actualResult = lower("fasFSsfja_SFASJFA")
    val expectResult = "fasfssfja_sfasjfa"
    assert(actualResult.equals(expectResult))
    assertThrows[NullPointerException](lower(null))
  }
}
