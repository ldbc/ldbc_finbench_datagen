package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.generation.DatagenParams
import ldbc.finbench.datagen.generation.events._
import ldbc.finbench.datagen.util.Logging
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters._
import scala.collection.SortedMap

class ActivityGenerator()(implicit spark: SparkSession)
    extends Serializable
    with Logging {
  // TODO: Move the type definitions to the some common place
  type EitherPersonOrCompany = Either[Person, Company]
  type EitherPersonInvestOrCompanyInvest =
    Either[PersonInvestCompany, CompanyInvestCompany]

  val blockSize: Int = DatagenParams.blockSize
  val sampleRandom =
    new scala.util.Random(
      DatagenParams.defaultSeed
    ) // Use a common sample random to avoid identical samples
  val accountGenerator =
    new AccountGenerator() // Use a common account generator to maintain the degree distribution
  val loanGenerator = new LoanGenerator()

  def personRegisterEvent(personRDD: RDD[Person]): RDD[Person] = {
    val blocks = personRDD.zipWithUniqueId().map(row => (row._2, row._1)).map {
      case (k, v) => (k / blockSize, (k, v))
    }
    val personRegisterEvent = new PersonRegisterEvent

    val personWithAccount = blocks
      .combineByKeyWithClassTag(
        personByRank => SortedMap(personByRank),
        (map: SortedMap[Long, Person], personByRank) => map + personByRank,
        (a: SortedMap[Long, Person], b: SortedMap[Long, Person]) => a ++ b
      )
      .mapPartitions(groups => {
        groups.flatMap { case (block, persons) =>
          personRegisterEvent
            .personRegister(
              persons.values.toList.asJava,
              accountGenerator,
              block.toInt
            )
            .iterator()
            .asScala
        }
      })
    personWithAccount
  }

  def companyRegisterEvent(companyRDD: RDD[Company]): RDD[Company] = {
    val blocks = companyRDD.zipWithUniqueId().map(row => (row._2, row._1)).map {
      case (k, v) => (k / blockSize, (k, v))
    }
    val companyRegisterGen = new CompanyRegisterEvent

    val companyWithAccount = blocks
      .combineByKeyWithClassTag(
        companyById => SortedMap(companyById),
        (map: SortedMap[Long, Company], companyById) => map + companyById,
        (a: SortedMap[Long, Company], b: SortedMap[Long, Company]) => a ++ b
      )
      .mapPartitions(groups => {
        groups.flatMap { case (block, companies) =>
          companyRegisterGen
            .companyRegister(
              companies.values.toList.asJava,
              accountGenerator,
              block.toInt
            )
            .iterator()
            .asScala
        }
      })
    companyWithAccount
  }

  // TODO: rewrite it with company centric
  def investEvent(
      personRDD: RDD[Person],
      companyRDD: RDD[Company]
  ): RDD[EitherPersonInvestOrCompanyInvest] = {
    val seedRandom = new scala.util.Random(DatagenParams.defaultSeed)
    val numInvestorsRandom = new scala.util.Random(DatagenParams.defaultSeed)
    val personInvestEvent = new PersonInvestEvent()
    val companyInvestEvent = new CompanyInvestEvent()

    // Sample some companies to be invested
    val investedCompanyRDD = companyRDD.sample(
      withReplacement = false,
      DatagenParams.companyInvestedFraction,
      sampleRandom.nextLong()
    )
    // Merge to either
    val personEitherRDD: RDD[EitherPersonOrCompany] =
      personRDD.map(person => Left(person))
    val companyEitherRDD: RDD[EitherPersonOrCompany] =
      companyRDD.map(company => Right(company))
    val mergedEither = personEitherRDD.union(companyEitherRDD).collect().toList

    // TODO: optimize the Spark process when large scale
    investedCompanyRDD.flatMap(investedCompany => {
      numInvestorsRandom.setSeed(seedRandom.nextInt())
      sampleRandom.setSeed(seedRandom.nextInt())
      personInvestEvent.resetState(seedRandom.nextInt())
      companyInvestEvent.resetState(seedRandom.nextInt())

      val numInvestors = numInvestorsRandom.nextInt(
        DatagenParams.maxInvestors - DatagenParams.minInvestors + 1
      ) + DatagenParams.minInvestors
      // Note: check if fraction 0.1 has enough numInvestors to take
      val investRels =
        sampleRandom.shuffle(mergedEither).take(numInvestors).map {
          case Left(person) =>
            Left(personInvestEvent.personInvest(person, investedCompany))
          case Right(company) =>
            Right(companyInvestEvent.companyInvest(company, investedCompany))
        }
      val ratioSum = investRels.map {
        case Left(personInvest)   => personInvest.getRatio
        case Right(companyInvest) => companyInvest.getRatio
      }.sum
      investRels.foreach {
        case Left(personInvest)   => personInvest.scaleRatio(ratioSum)
        case Right(companyInvest) => companyInvest.scaleRatio(ratioSum)
      }
      investRels
    })
  }

  def signInEvent(
      mediumRDD: RDD[Medium],
      accountRDD: RDD[Account]
  ): RDD[SignIn] = {
    val accountSampleList = accountRDD
      .sample(
        withReplacement = false,
        DatagenParams.accountSignedInFraction,
        sampleRandom.nextLong()
      )
      .collect()
      .grouped(accountRDD.partitions.length)
      .map(_.toList.asJava)
      .toList
      .asJava

    val signInEvent = new SignInEvent
    mediumRDD.mapPartitionsWithIndex((index, mediums) => {
      signInEvent
        .signIn(
          mediums.toList.asJava,
          accountSampleList.get(index),
          index
        )
        .iterator()
        .asScala
    })
  }

  def personGuaranteeEvent(personRDD: RDD[Person]): RDD[Person] = {
    val personGuaranteeEvent = new PersonGuaranteeEvent
    personRDD.mapPartitionsWithIndex((index, persons) => {
      personGuaranteeEvent
        .personGuarantee(
          persons.toList.asJava,
          index
        )
        .iterator()
        .asScala
    })
  }

  def companyGuaranteeEvent(companyRDD: RDD[Company]): RDD[Company] = {
    val companyGuaranteeEvent = new CompanyGuaranteeEvent
    companyRDD.mapPartitionsWithIndex((index, companies) => {
      companyGuaranteeEvent
        .companyGuarantee(
          companies.toList.asJava,
          index
        )
        .iterator()
        .asScala
    })
  }

  def personLoanEvent(personRDD: RDD[Person]): RDD[Person] = {
    val personLoanEvent = new PersonLoanEvent
    personRDD.mapPartitionsWithIndex((index, persons) => {
      personLoanEvent
        .personLoan(
          persons.toList.asJava,
          loanGenerator,
          index
        )
        .iterator()
        .asScala
    })
  }

  def companyLoanEvent(companyRDD: RDD[Company]): RDD[Company] = {
    val companyLoanEvent = new CompanyLoanEvent
    companyRDD.mapPartitionsWithIndex((index, companies) => {
      companyLoanEvent
        .companyLoan(
          companies.toList.asJava,
          loanGenerator,
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
    val fraction = DatagenParams.loanInvolvedAccountsFraction
    val loanParts = loanRDD.partitions.length
    val accountSampleList = new util.ArrayList[util.List[Account]](loanParts)
    for (i <- 1 to loanParts) {
      val sampleAccounts = accountRDD.sample(
        withReplacement = false,
        fraction / loanParts,
        sampleRandom.nextLong()
      )
      accountSampleList.add(sampleAccounts.collect().toList.asJava)
    }

    // TODO: optimize the map function with the Java-Scala part.
    val afterLoanActions = loanRDD.mapPartitions(loans => {
      val partitionId = TaskContext.getPartitionId()
      val loanSubEvents = new LoanSubEvents(accountSampleList.get(partitionId))
      loanSubEvents.afterLoanApplied(loans.toList.asJava, partitionId)
      Iterator(
        (
          loanSubEvents.getDeposits.asScala,
          loanSubEvents.getRepays.asScala,
          loanSubEvents.getTransfers.asScala
        )
      )
    })

    val deposits = afterLoanActions.map(_._1).flatMap(deposits => deposits)
    val repays = afterLoanActions.map(_._2).flatMap(repays => repays)
    val transfers = afterLoanActions.map(_._3).flatMap(transfers => transfers)

    (deposits, repays, transfers)
  }
}
