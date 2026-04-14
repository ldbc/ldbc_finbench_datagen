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

package ldbc.finbench.datagen.generation

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.generation.generators.{ActivityGenerator, SparkCompanyGenerator, SparkMediumGenerator, SparkPersonGenerator}
import ldbc.finbench.datagen.generation.serializers.ActivitySerializer
import ldbc.finbench.datagen.io.Writer
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.util.Logging
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ActivitySimulator(sink: RawSink)(implicit spark: SparkSession)
    extends Writer[RawSink]
    with Serializable
    with Logging {
  private val blockSize: Int = DatagenParams.blockSize
  private val activityGenerator = new ActivityGenerator()
  private val activitySerializer = new ActivitySerializer(sink)

  def simulate(config: DatagenConfiguration): Unit = {
    val personRdd =
      SparkPersonGenerator(DatagenParams.numPersons, config, blockSize)
    val companyRdd =
      SparkCompanyGenerator(DatagenParams.numCompanies, config, blockSize)
    val mediumRdd =
      SparkMediumGenerator(DatagenParams.numMediums, config, blockSize)

    val personWithAccGuaLoan = activityGenerator.personActivitiesEvent(personRdd)
    val companyWithAccGuaLoan = activityGenerator.companyActivitiesEvent(companyRdd)
    val companyRddAfterInvest = activityGenerator.investEvent(personRdd, companyRdd)

    Await.result(
      Future.sequence(activitySerializer.writePersonWithActivities(personWithAccGuaLoan)),
      Duration.Inf
    )
    Await.result(
      Future.sequence(activitySerializer.writeCompanyWithActivities(companyWithAccGuaLoan)),
      Duration.Inf
    )
    Await.result(
      Future.sequence(activitySerializer.writeInvestCompanies(companyRddAfterInvest)),
      Duration.Inf
    )

    val accountRdd = mergeAccountsAndShuffleDegrees(personWithAccGuaLoan, companyWithAccGuaLoan)

    val mediumWithSignInRdd = activityGenerator.mediumActivitesEvent(mediumRdd, accountRdd)
    Await.result(
      Future.sequence(activitySerializer.writeMediumWithActivities(mediumWithSignInRdd)),
      Duration.Inf
    )

    val accountWithTransferWithdraw =
      activityGenerator.accountActivitiesEvent(accountRdd).persist(StorageLevel.DISK_ONLY)

    Await.result(
      Future.sequence(activitySerializer.writeAccountWithActivities(accountWithTransferWithdraw)),
      Duration.Inf
    )
    accountWithTransferWithdraw.unpersist(blocking = true)


    val loanRdd = mergeLoans(personWithAccGuaLoan, companyWithAccGuaLoan)
    val loanWithActivitiesRdd = 
      activityGenerator.afterLoanSubEvents(loanRdd, accountRdd).persist(StorageLevel.DISK_ONLY)

    Await.result(
      Future.sequence(activitySerializer.writeLoanActivities(loanWithActivitiesRdd)),
      Duration.Inf
    )
    loanWithActivitiesRdd.unpersist(blocking = true)
  }

  private def mergeAccountsAndShuffleDegrees(
      persons: RDD[Person],
      companies: RDD[Company]
  ): RDD[Account] = {
    val personAccounts =
      persons.flatMap(_.getAccount.asScala)
    val companyAccounts =
      companies.flatMap(_.getAccount.asScala)
    personAccounts
      .union(companyAccounts)
      .mapPartitions { iter =>
        val accounts = iter.toArray
        shuffleDegrees(accounts)
        accounts.iterator
      }
  }

  private def shuffleDegrees(accounts: Array[Account]): Unit = {
    val shuffledInDegrees = new Array[Long](accounts.length)
    var index = 0
    while (index < accounts.length) {
      shuffledInDegrees(index) = accounts(index).getMaxInDegree
      index += 1
    }

    val random = new scala.util.Random(TaskContext.getPartitionId())
    var i = shuffledInDegrees.length - 1
    while (i > 0) {
      val j = random.nextInt(i + 1)
      val tmp = shuffledInDegrees(i)
      shuffledInDegrees(i) = shuffledInDegrees(j)
      shuffledInDegrees(j) = tmp
      i -= 1
    }

    index = 0
    while (index < accounts.length) {
      accounts(index).setMaxOutDegree(shuffledInDegrees(index))
      index += 1
    }
  }

  private def mergeLoans(
      persons: RDD[Person],
      companies: RDD[Company]
  ): RDD[Loan] = {
    val personLoans =
      persons.flatMap(_.getLoan.asScala)
    val companyLoans =
      companies.flatMap(_.getLoan.asScala)
    personLoans.union(companyLoans)
  }
}
