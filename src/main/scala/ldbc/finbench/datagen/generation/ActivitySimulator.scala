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
    log.info(
      s"[Simulation] Person RDD partitions: ${personRdd.getNumPartitions}, "
        + s"Company RDD partitions: ${companyRdd.getNumPartitions}, "
        + s"Medium RDD partitions: ${mediumRdd.getNumPartitions}"
    )

    val personWithAccGuaLoan = activityGenerator.personActivitiesEvent(personRdd)
    val companyWithAccGuaLoan = activityGenerator.companyActivitiesEvent(companyRdd)
    log.info(
      s"[Simulation] personWithAccGuaLoan partitions: ${personWithAccGuaLoan.getNumPartitions}, "
        + s"companyWithAccGuaLoan partitions: ${companyWithAccGuaLoan.getNumPartitions}"
    )
    val companyRddAfterInvest = activityGenerator.investEvent(personRdd, companyRdd)

    val accountRdd = mergeAccountsAndShuffleDegrees(personWithAccGuaLoan, companyWithAccGuaLoan)
    val mediumWithSignInRdd = activityGenerator.mediumActivitesEvent(mediumRdd, accountRdd)
    val accountWithTransferWithdraw = activityGenerator.accountActivitiesEvent(accountRdd)
    log.info(
      s"[Simulation] Account RDD partitions: ${accountRdd.getNumPartitions}"
        + s"[Simulation] signIn RDD partitions: ${mediumWithSignInRdd.getNumPartitions}"
    )

    val loanRdd = mergeLoans(personWithAccGuaLoan, companyWithAccGuaLoan)
    val loanWithActivitiesRdd = activityGenerator.afterLoanSubEvents(loanRdd, accountRdd)
    log.info(s"[Simulation] Loan RDD partitions: ${loanWithActivitiesRdd.getNumPartitions}")

    // Serialize
    val allFutures = Seq(
      activitySerializer.writePersonWithActivities(personWithAccGuaLoan),
      activitySerializer.writeCompanyWithActivities(companyWithAccGuaLoan),
      activitySerializer.writeMediumWithActivities(mediumWithSignInRdd),
      activitySerializer.writeAccountWithActivities(accountWithTransferWithdraw),
      activitySerializer.writeInvestCompanies(companyRddAfterInvest),
      activitySerializer.writeLoanActivities(loanWithActivitiesRdd)
      ).flatten

    Await.result(Future.sequence(allFutures), Duration.Inf)
  }

  private def mergeAccountsAndShuffleDegrees(
      persons: RDD[Person],
      companies: RDD[Company]
  ): RDD[Account] = {
    val personAccounts =
      persons.flatMap(_.getPersonOwnAccounts.asScala.map(_.getAccount))
    val companyAccounts =
      companies.flatMap(_.getCompanyOwnAccounts.asScala.map(_.getAccount))
    personAccounts
      .union(companyAccounts)
      .mapPartitions(iter => shuffleDegrees(iter.toList).iterator)
  }

  private def shuffleDegrees(accounts: List[Account]): List[Account] = {
    val indegrees = accounts.map(_.getMaxInDegree)
    val shuffled =
      new scala.util.Random(TaskContext.getPartitionId()).shuffle(indegrees)
    accounts.zip(shuffled).foreach { case (account, shuffled) =>
      account.setMaxOutDegree(shuffled)
    }
    accounts
  }

  private def mergeLoans(
      persons: RDD[Person],
      companies: RDD[Company]
  ): RDD[Loan] = {
    val personLoans =
      persons.flatMap(_.getPersonApplyLoans.asScala.map(_.getLoan))
    val companyLoans =
      companies.flatMap(_.getCompanyApplyLoans.asScala.map(_.getLoan))
    personLoans.union(companyLoans)
  }
}
