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
