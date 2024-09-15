package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.generation.DatagenParams
import ldbc.finbench.datagen.generation.events._
import ldbc.finbench.datagen.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.SortedMap

class ActivityGenerator()(implicit spark: SparkSession)
    extends Serializable
    with Logging {

  val blockSize: Int = DatagenParams.blockSize
  val sampleRandom = new scala.util.Random(DatagenParams.defaultSeed)
  val accountGenerator = new AccountGenerator()
  val loanGenerator = new LoanGenerator()

  // including account, loan, guarantee
  def personActivitiesEvent(personRDD: RDD[Person]): RDD[Person] = {
    val personActivitiesEvent = new PersonActivitiesEvent
    val blocks = personRDD.zipWithUniqueId().map(row => (row._2, row._1)).map {
      case (k, v) => (k / blockSize, (k, v))
    }

    val personWithAccountsLoansGuarantees = blocks
      .combineByKeyWithClassTag(
        personByRank => SortedMap(personByRank),
        (map: SortedMap[Long, Person], personByRank) => map + personByRank,
        (a: SortedMap[Long, Person], b: SortedMap[Long, Person]) => a ++ b
      )
      .mapPartitions(groups => {
        groups.flatMap { case (block, persons) =>
          personActivitiesEvent
            .personActivities(
              persons.values.toList.asJava,
              accountGenerator,
              loanGenerator,
              block.toInt
            )
            .iterator()
            .asScala
        }
      })

    personWithAccountsLoansGuarantees
  }

  def companyActivitiesEvent(companyRDD: RDD[Company]): RDD[Company] = {
    val companyActivitiesEvent = new CompanyActivitiesEvent
    val blocks = companyRDD.zipWithUniqueId().map(row => (row._2, row._1)).map {
      case (k, v) => (k / blockSize, (k, v))
    }

    val companyWithAccountsLoansGuarantees = blocks
      .combineByKeyWithClassTag(
        companyByRank => SortedMap(companyByRank),
        (map: SortedMap[Long, Company], companyByRank) => map + companyByRank,
        (a: SortedMap[Long, Company], b: SortedMap[Long, Company]) => a ++ b
      )
      .mapPartitions(groups => {
        groups.flatMap { case (block, companies) =>
          companyActivitiesEvent
            .companyActivities(
              companies.values.toList.asJava,
              accountGenerator,
              loanGenerator,
              block.toInt
            )
            .iterator()
            .asScala
        }
      })

    companyWithAccountsLoansGuarantees
  }

  def investEvent(
      personRDD: RDD[Person],
      companyRDD: RDD[Company]
  ): RDD[Company] = {
    val persons = spark.sparkContext.broadcast(personRDD.collect().toList)
    val companies = spark.sparkContext.broadcast(companyRDD.collect().toList)

    val personInvestEvent = new PersonInvestEvent()
    val companyInvestEvent = new CompanyInvestEvent()

    companyRDD
      .sample(
        withReplacement = false,
        DatagenParams.companyInvestedFraction,
        sampleRandom.nextLong()
      )
      .mapPartitionsWithIndex { (index, targets) =>
        personInvestEvent.resetState(index)
        personInvestEvent
          .personInvestPartition(persons.value.asJava, targets.toList.asJava)
          .iterator()
          .asScala
      }
      .mapPartitionsWithIndex { (index, targets) =>
        companyInvestEvent.resetState(index)
        companyInvestEvent
          .companyInvestPartition(
            companies.value.asJava,
            targets.toList.asJava
          )
          .iterator()
          .asScala
      }
      .map(_.scaleInvestmentRatios())
  }

  def signInEvent(
      mediumRDD: RDD[Medium],
      accountRDD: RDD[Account]
  ): RDD[SignIn] = {
    val accountSampleList = spark.sparkContext.broadcast(
      accountRDD
        .sample(
          withReplacement = false,
          DatagenParams.accountSignedInFraction,
          sampleRandom.nextLong()
        )
        .collect()
        .toList
    )

    val signInEvent = new SignInEvent
    mediumRDD.mapPartitionsWithIndex((index, mediums) => {
      signInEvent
        .signIn(
          mediums.toList.asJava,
          accountSampleList.value.asJava,
          index
        )
        .iterator()
        .asScala
    })
  }

  // Tries: StackOverflowError caused when merging accounts RDD causes, maybe due to deep RDD lineage
  // TODO: rewrite it with account centric and figure out the StackOverflowError
  def transferEvent(accountRDD: RDD[Account]): RDD[Transfer] = {
    val transferEvent = new TransferEvent

    Array
      .fill(DatagenParams.transferShuffleTimes) {
        accountRDD
          .repartition(accountRDD.getNumPartitions)
          .mapPartitionsWithIndex((index, accounts) => {
            transferEvent
              .transferPart(accounts.toList.asJava, index)
              .iterator()
              .asScala
          })
      }
      .reduce(_ union _)
  }

  // TODO: rewrite it with account centric
  def withdrawEvent(accountRDD: RDD[Account]): RDD[Withdraw] = {
    val withdrawEvent = new WithdrawEvent

    val cards = accountRDD.filter(_.getType == "debit card").collect()
    accountRDD
      .filter(_.getType != "debit card")
      .sample(
        withReplacement = false,
        DatagenParams.accountWithdrawFraction,
        sampleRandom.nextLong()
      )
      .mapPartitionsWithIndex((index, sources) => {
        withdrawEvent
          .withdraw(
            sources.toList.asJava,
            cards.toList.asJava,
            index
          )
          .iterator()
          .asScala
      })
  }

  // TODO: rewrite it with loan centric
  def afterLoanSubEvents(
      loanRDD: RDD[Loan],
      accountRDD: RDD[Account]
  ): (RDD[Deposit], RDD[Repay], RDD[Transfer]) = {
    val sampledAccounts = spark.sparkContext.broadcast(
      accountRDD
        .sample(
          withReplacement = false,
          DatagenParams.loanInvolvedAccountsFraction,
          sampleRandom.nextLong()
        )
        .collect()
        .toList
    )

    // TODO: optimize the map function with the Java-Scala part.
    val afterLoanActions = loanRDD
      .mapPartitionsWithIndex((index, loans) => {
        val loanSubEvents = new LoanSubEvents(sampledAccounts.value.asJava)
        loanSubEvents.afterLoanApplied(loans.toList.asJava, index)
        Iterator(
          (
            loanSubEvents.getDeposits.asScala,
            loanSubEvents.getRepays.asScala,
            loanSubEvents.getTransfers.asScala
          )
        )
      })
      .cache()

    (
      afterLoanActions.flatMap(_._1),
      afterLoanActions.flatMap(_._2),
      afterLoanActions.flatMap(_._3)
    )
  }
}
