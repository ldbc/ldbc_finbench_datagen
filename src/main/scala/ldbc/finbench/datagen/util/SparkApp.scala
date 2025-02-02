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

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApp {
  def appName: String

  type ArgsType

  /** execute the data generation process
    */
  def run(args: ArgsType): Unit

  /** set the {@@linkSparkConf}
    */
  val sparkConf = setConf(new SparkConf(), defaultSparkConf)

  /** spark entry {@@linkSparkSession}
    */
  implicit def spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName(appName)
      .config(sparkConf)
      .getOrCreate()

  private def applySparkConf(sparkConf: Map[String, String])(
      builder: SparkSession.Builder
  ) =
    sparkConf.foldLeft(builder) { case (b, (k, v)) => b.config(k, v) }

  def setConf(sparkConf: SparkConf, conf: Map[String, String]): SparkConf = {
    conf.map(entry => {
      if (!sparkConf.contains(entry._1)) {
        sparkConf.set(entry._1, entry._2)
      }
    })
    registerKyroClasses(sparkConf)
  }

  def registerKyroClasses(sparkConf: SparkConf): SparkConf = {
    // register kryo classes for nodes
    sparkConf.registerKryoClasses(
      Array(
        classOf[Account],
        classOf[Company],
        classOf[Loan],
        classOf[Medium],
        classOf[Person]
      )
    )
    // register kryo classes for edges
    sparkConf.registerKryoClasses(
      Array(
        classOf[CompanyApplyLoan],
        classOf[CompanyGuaranteeCompany],
        classOf[CompanyInvestCompany],
        classOf[CompanyOwnAccount],
        classOf[PersonApplyLoan],
        classOf[PersonGuaranteePerson],
        classOf[PersonInvestCompany],
        classOf[PersonOwnAccount],
        classOf[Repay],
        classOf[SignIn],
        classOf[Transfer],
        classOf[Withdraw]
      )
    )
    sparkConf
  }

  def defaultSparkConf: Map[String, String] = Map(
    "spark.sql.session.timeZone" -> "GMT",
    "spark.sql.sources.useV1SourceList" -> "csv"
  )

  protected lazy val env: SparkEnv = new SparkEnv

}

trait DatagenStage extends SparkApp {
  override val appName: String =
    s"LDBC Finbench Datagen for Spark: ${this.getClass.getSimpleName.stripSuffix("$")}"
}
