package ldbc.finbench.datagen.generation

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.generation.generators.{
  ActivityGenerator,
  SparkCompanyGenerator,
  SparkMediumGenerator,
  SparkPersonGenerator
}
import ldbc.finbench.datagen.generation.serializers.ActivitySerializer
import ldbc.finbench.datagen.io.Writer
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.util.Logging
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

// TODO:
//  - refactor using common GraphDef to make the code less verbose
//  - repartition with the partition option
class ActivitySimulator(sink: RawSink)(implicit spark: SparkSession)
    extends Writer[RawSink]
    with Serializable
    with Logging {
  private val parallelism = spark.sparkContext.defaultParallelism
  private val blockSize: Int = DatagenParams.blockSize

  private val activityGenerator = new ActivityGenerator()
  private val activitySerializer = new ActivitySerializer(sink)

  private val personNum: Long = DatagenParams.numPersons
  private val personPartitions = Some(
    Math
      .min(Math.ceil(personNum.toDouble / blockSize).toLong, parallelism)
      .toInt
  )

  private val companyNum: Long = DatagenParams.numCompanies
  private val companyPartitions = Some(
    Math
      .min(Math.ceil(companyNum.toDouble / blockSize).toLong, parallelism)
      .toInt
  )

  private val mediumNum: Long = DatagenParams.numMediums
  private val mediumPartitions = Some(
    Math
      .min(Math.ceil(mediumNum.toDouble / blockSize).toLong, parallelism)
      .toInt
  )

  def simulate(config: DatagenConfiguration): Unit = {
    val personRdd: RDD[Person] =
      SparkPersonGenerator(personNum, config, blockSize, personPartitions)
    val companyRdd: RDD[Company] =
      SparkCompanyGenerator(companyNum, config, blockSize, companyPartitions)
    val mediumRdd: RDD[Medium] =
      SparkMediumGenerator(mediumNum, config, blockSize, mediumPartitions)
    log.info(
      s"[Simulation] Person RDD partitions: ${personRdd.getNumPartitions}"
    )
    log.info(
      s"[Simulation] Company RDD partitions: ${companyRdd.getNumPartitions}"
    )
    log.info(
      s"[Simulation] Medium RDD partitions: ${mediumRdd.getNumPartitions}"
    )

    // Person and company related activities
    val personWithAccounts = activityGenerator.personRegisterEvent(
      personRdd
    ) // simulate person register event
    log.info(
      s"[Simulation] personWithAccounts partitions: ${personWithAccounts.getNumPartitions}"
    )
    val companyWithAccounts = activityGenerator.companyRegisterEvent(
      companyRdd
    ) // simulate company register event
    log.info(
      s"[Simulation] companyWithAccounts partitions: ${companyWithAccounts.getNumPartitions}"
    )

    // simulate person or company invest company event
    val investRdd = activityGenerator.investEvent(personRdd, companyRdd)
    log.info(
      s"[Simulation] invest RDD partitions: ${investRdd.getNumPartitions}"
    )

    // simulate person guarantee person event and company guarantee company event
    val personWithAccGua =
      activityGenerator.personGuaranteeEvent(personWithAccounts)
    val companyWitAccGua =
      activityGenerator.companyGuaranteeEvent(companyWithAccounts)
    log.info(
      s"[Simulation] personWithAccGua partitions: ${personWithAccGua.getNumPartitions}"
    )
    log.info(
      s"[Simulation] companyWitAccGua partitions: ${companyWitAccGua.getNumPartitions}"
    )

    // simulate person apply loans event and company apply loans event
    val personWithAccGuaLoan =
      activityGenerator.personLoanEvent(personWithAccGua).cache()
    val companyWithAccGuaLoan =
      activityGenerator.companyLoanEvent(companyWitAccGua).cache()
//    assert(personWithAccGuaLoan.count() == personRdd.count())
//    assert(companyWithAccGuaLoan.count() == companyRdd.count())
    log.info(
      s"[Simulation] personWithAccGuaLoan partitions: ${personWithAccGuaLoan.getNumPartitions}"
    )
    log.info(
      s"[Simulation] companyWithAccGuaLoan partitions: ${companyWithAccGuaLoan.getNumPartitions}"
    )

    // Account related activities
    val accountRdd =
      mergeAccounts(personWithAccounts, companyWithAccounts) // merge
    log.info(
      s"[Simulation] Account RDD partitions: ${accountRdd.getNumPartitions}"
    )
    val signInRdd =
      activityGenerator.signInEvent(mediumRdd, accountRdd) // simulate signIn
    val mergedTransfers =
      activityGenerator.transferEvent(accountRdd) // simulate transfer
    val withdrawRdd =
      activityGenerator.withdrawEvent(accountRdd) // simulate withdraw
    log.info(
      s"[Simulation] signIn RDD partitions: ${signInRdd.getNumPartitions}"
    )
    log.info(
      s"[Simulation] transfer RDD partitions: ${mergedTransfers.getNumPartitions}"
    )
    log.info(
      s"[Simulation] withdraw RDD partitions: ${withdrawRdd.getNumPartitions}"
    )

    // =========================================
    // Loan related activities
    // =========================================
    val loanRdd =
      mergeLoans(personWithAccGuaLoan, companyWithAccGuaLoan) // merge
    log.info(s"[Simulation] Loan RDD partitions: ${loanRdd.getNumPartitions}")
    val (depositsRdd, repaysRdd, loanTrasfersRdd) =
      activityGenerator.afterLoanSubEvents(loanRdd, accountRdd)
    log.info(
      s"[Simulation] deposits RDD partitions: ${depositsRdd.getNumPartitions}, " +
        s"repays RDD partitions: ${repaysRdd.getNumPartitions}, " +
        s"loanTrasfers RDD partitions: ${loanTrasfersRdd.getNumPartitions}"
    )

    // Serialize
    val allFutures =
      activitySerializer.writePersonWithActivities(personWithAccGuaLoan) ++
        activitySerializer.writeCompanyWithActivities(companyWithAccGuaLoan) ++
        activitySerializer.writeMediumWithActivities(mediumRdd, signInRdd) ++
        activitySerializer.writeAccountWithActivities(
          accountRdd,
          mergedTransfers
        ) ++
        activitySerializer.writeWithdraw(withdrawRdd) ++
        activitySerializer.writeInvest(investRdd) ++
        activitySerializer.writeLoanActivities(
          loanRdd,
          depositsRdd,
          repaysRdd,
          loanTrasfersRdd
        )

    Await.result(Future.sequence(allFutures), Duration.Inf)
  }

  private def mergeAccounts(
      persons: RDD[Person],
      companies: RDD[Company]
  ): RDD[Account] = {
    val personAccounts = persons.flatMap(person =>
      person.getPersonOwnAccounts.asScala.map(_.getAccount)
    )
    val companyAccounts = companies.flatMap(company =>
      company.getCompanyOwnAccounts.asScala.map(_.getAccount)
    )
    val allAccounts = personAccounts
      .union(companyAccounts)
      .mapPartitions(iter => shuffleDegrees(iter.toList).iterator)
    allAccounts
  }

  private def shuffleDegrees(accounts: List[Account]): List[Account] = {
    val indegrees = accounts.map(_.getMaxInDegree)
    val shuffled =
      new scala.util.Random(TaskContext.getPartitionId()).shuffle(indegrees)
    accounts.zip(shuffled).foreach { case (account, shuffled) =>
      account.setMaxOutDegree(shuffled)
      account.setRawMaxOutDegree(shuffled)
    }
    accounts
  }

  private def mergeLoans(
      persons: RDD[Person],
      companies: RDD[Company]
  ): RDD[Loan] = {
    val personLoans = persons.flatMap(person => person.getLoans.asScala)
    val companyLoans = companies.flatMap(company => company.getLoans.asScala)
    personLoans.union(companyLoans)
  }
}
