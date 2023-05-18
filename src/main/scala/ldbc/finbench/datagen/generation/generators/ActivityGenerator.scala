package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.generation.DatagenParams
import ldbc.finbench.datagen.generation.events._
import ldbc.finbench.datagen.util.Logging
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import java.util
import scala.collection.JavaConverters._
import scala.collection.SortedMap

class ActivityGenerator() extends Serializable with Logging {
  // TODO: Move the type definitions to the some common place
  type EitherPersonOrCompany = Either[Person, Company]
  type EitherPersonInvestOrCompanyInvest = Either[PersonInvestCompany, CompanyInvestCompany]

  val blockSize: Int = DatagenParams.blockSize
  val sampleRandom = new scala.util.Random(DatagenParams.defaultSeed) // Use a common sample random to avoid identical samples
  val accountGenerator = new AccountGenerator() // Use a common account generator to maintain the degree distribution
  val loanGenerator = new LoanGenerator()

  def personRegisterEvent(personRDD: RDD[Person]): RDD[PersonOwnAccount] = {
    val blocks = personRDD.zipWithUniqueId().map(row => (row._2, row._1)).map { case (k, v) => (k / blockSize, (k, v)) }
    val personRegisterGen = new PersonRegisterEvent

    val personOwnAccount = blocks.combineByKeyWithClassTag(
      personByRank => SortedMap(personByRank),
      (map: SortedMap[Long, Person], personByRank) => map + personByRank,
      (a: SortedMap[Long, Person], b: SortedMap[Long, Person]) => a ++ b
    )
      .mapPartitions(groups => {
        val personRegisterGroups = for {(block, persons) <- groups} yield {
          personRegisterGen.personRegister(persons.values.toList.asJava, accountGenerator, block.toInt)
        }

        for {
          personOwnAccounts <- personRegisterGroups
          personOwnAccount <- personOwnAccounts.iterator().asScala
        } yield personOwnAccount
      })
    personOwnAccount
  }

  def companyRegisterEvent(companyRDD: RDD[Company]): RDD[CompanyOwnAccount] = {
    val blocks = companyRDD.zipWithUniqueId().map(row => (row._2, row._1)).map { case (k, v) => (k / blockSize, (k, v)) }
    val companyRegisterGen = new CompanyRegisterEvent

    val companyOwnAccount = blocks.combineByKeyWithClassTag(
      companyById => SortedMap(companyById),
      (map: SortedMap[Long, Company], companyById) => map + companyById,
      (a: SortedMap[Long, Company], b: SortedMap[Long, Company]) => a ++ b
    )
      .mapPartitions(groups => {
        val companyRegisterGroups = for {(block, companies) <- groups} yield {
          companyRegisterGen.companyRegister(companies.values.toList.asJava, accountGenerator, block.toInt)
        }

        for {
          companyOwnAccounts <- companyRegisterGroups
          companyOwnAccount <- companyOwnAccounts.iterator().asScala
        } yield companyOwnAccount
      })
    companyOwnAccount
  }

  def investEvent(personRDD: RDD[Person], companyRDD: RDD[Company]): RDD[EitherPersonInvestOrCompanyInvest] = {
    val seedRandom = new scala.util.Random(DatagenParams.defaultSeed)
    val numInvestorsRandom = new scala.util.Random(DatagenParams.defaultSeed)
    val personInvestEvent = new PersonInvestEvent()
    val companyInvestEvent = new CompanyInvestEvent()

    // Sample some companies to be invested
    val investedCompanyRDD = companyRDD.sample(withReplacement = false, DatagenParams.companyInvestedFraction, sampleRandom.nextLong())
    // Merge to either
    val personEitherRDD: RDD[EitherPersonOrCompany] = personRDD.map(person => Left(person))
    val companyEitherRDD: RDD[EitherPersonOrCompany] = companyRDD.map(company => Right(company))
    val mergedEither = personEitherRDD.union(companyEitherRDD).collect().toList

    // TODO: optimize the Spark process when large scale
    investedCompanyRDD.map(investedCompany => {
      numInvestorsRandom.setSeed(seedRandom.nextInt())
      sampleRandom.setSeed(seedRandom.nextInt())
      personInvestEvent.resetState(seedRandom.nextInt())
      companyInvestEvent.resetState(seedRandom.nextInt())

      val numInvestors = numInvestorsRandom.nextInt(DatagenParams.maxInvestors - DatagenParams.minInvestors + 1) + DatagenParams.minInvestors
      // Note: check if fraction 0.1 has enough numInvestors to take
      val investRels = sampleRandom.shuffle(mergedEither).take(numInvestors).map {
        case Left(person) => Left(personInvestEvent.personInvest(person, investedCompany))
        case Right(company) => Right(companyInvestEvent.companyInvest(company, investedCompany))
      }
      val ratioSum = investRels.map {
        case Left(personInvest) => personInvest.getRatio
        case Right(companyInvest) => companyInvest.getRatio
      }.sum
      investRels.foreach {
        case Left(personInvest) => personInvest.scaleRatio(ratioSum)
        case Right(companyInvest) => companyInvest.scaleRatio(ratioSum)
      }
      investRels
    }
    ).flatMap(investRels => investRels)
  }

  def signInEvent(mediumRDD: RDD[Medium], accountRDD: RDD[Account]): RDD[SignIn] = {
    val mediumParts = mediumRDD.partitions.length
    val accountSampleList = new util.ArrayList[util.List[Account]](mediumParts)
    for (i <- 1 to mediumParts) {
      val sampleAccounts = accountRDD.sample(withReplacement = false, DatagenParams.accountSignedInFraction / mediumParts, sampleRandom.nextLong())
      accountSampleList.add(sampleAccounts.collect().toList.asJava)
    }

    val signInEvent = new SignInEvent
    val signRels = mediumRDD.mapPartitions(mediums => {
      val partitionId = TaskContext.getPartitionId()
      val signInList = signInEvent.signIn(mediums.toList.asJava, accountSampleList.get(partitionId), partitionId)
      for {signIn <- signInList.iterator().asScala} yield signIn
    })

    signRels
  }

  def personGuaranteeEvent(personRDD: RDD[Person]): RDD[PersonGuaranteePerson] = {
    val personGuaranteeEvent = new PersonGuaranteeEvent
    val personSample = personRDD.sample(withReplacement = false, DatagenParams.personGuaranteeFraction, sampleRandom.nextLong())
    personSample.mapPartitions(persons => {
      val guaranteeList = personGuaranteeEvent.personGuarantee(persons.toList.asJava, TaskContext.getPartitionId())
      for {guarantee <- guaranteeList.iterator().asScala} yield guarantee
    })
  }

  def companyGuaranteeEvent(companyRDD: RDD[Company]): RDD[CompanyGuaranteeCompany] = {
    val companyGuaranteeEvent = new CompanyGuaranteeEvent
    val companySample = companyRDD.sample(withReplacement = false, DatagenParams.companyGuaranteeFraction, sampleRandom.nextLong())
    companySample.mapPartitions(companies => {
      val guaranteeList = companyGuaranteeEvent.companyGuarantee(companies.toList.asJava, TaskContext.getPartitionId())
      for {guarantee <- guaranteeList.iterator().asScala} yield guarantee
    })
  }

  def personLoanEvent(personRDD: RDD[Person]): RDD[PersonApplyLoan] = {
    log.info(s"personLoanEvent start. NumPartitions: ${personRDD.getNumPartitions}")
    val personLoanEvent = new PersonLoanEvent
    val personSample = personRDD.sample(withReplacement = false, DatagenParams.personLoanFraction, sampleRandom.nextLong())
    personSample.mapPartitions(persons => {
      val loanList = personLoanEvent.personLoan(persons.toList.asJava, loanGenerator, TaskContext.getPartitionId())
      for {applyLoan <- loanList.iterator().asScala} yield applyLoan
    })
  }

  def companyLoanEvent(companyRDD: RDD[Company]): RDD[CompanyApplyLoan] = {
    val companyLoanEvent = new CompanyLoanEvent
    val companySample = companyRDD.sample(withReplacement = false, DatagenParams.companyLoanFraction, sampleRandom.nextLong())
    companySample.mapPartitions(companies => {
      val loanList = companyLoanEvent.companyLoan(companies.toList.asJava, loanGenerator, TaskContext.getPartitionId())
      for {applyLoan <- loanList.iterator().asScala} yield applyLoan
    })
  }

  def transferEvent(accountRDD: RDD[Account]): RDD[Transfer] = {
    val transferEvent = new TransferEvent
    accountRDD.mapPartitions(accounts => {
      val transferList = transferEvent.transfer(accounts.toList.asJava, TaskContext.getPartitionId())
      for {transfer <- transferList.iterator().asScala} yield transfer
    })
  }

  def withdrawEvent(accountRDD: RDD[Account]): RDD[Withdraw] = {
    val withdrawEvent = new WithdrawEvent
    val sourceSamples = accountRDD.filter(_.getType != "debit card").sample(withReplacement = false, DatagenParams.accountWithdrawFraction, sampleRandom.nextLong())
    val cards = accountRDD.filter(_.getType == "debit card").collect().toList.asJava
    sourceSamples.mapPartitions(sources => {
      val withdrawList = withdrawEvent.withdraw(sources.toList.asJava, cards, TaskContext.getPartitionId())
      for {withdraw <- withdrawList.iterator().asScala} yield withdraw
    })
  }

  def afterLoanSubEvents(loanRDD: RDD[Loan], accountRDD: RDD[Account]): (RDD[Deposit], RDD[Repay], RDD[Transfer]) = {
    val fraction = DatagenParams.loanInvolvedAccountsFraction
    val loanParts = loanRDD.partitions.length
    val accountSampleList = new util.ArrayList[util.List[Account]](loanParts)
    for (i <- 1 to loanParts) {
      val sampleAccounts = accountRDD.sample(withReplacement = false, fraction / loanParts, sampleRandom.nextLong())
      accountSampleList.add(sampleAccounts.collect().toList.asJava)
    }

    log.info(s"depositAndRepayEvent start. NumPartitions: ${loanRDD.getNumPartitions}")

    // TODO: optimize the map function with the Java-Scala part.
    val afterLoanActions = loanRDD.mapPartitions(loans => {
      val partitionId = TaskContext.getPartitionId()
      val loanSubEvents = new LoanSubEvents(accountSampleList.get(partitionId))
      loanSubEvents.afterLoanApplied(loans.toList.asJava, partitionId)
      Iterator((loanSubEvents.getDeposits.asScala,
        loanSubEvents.getRepays.asScala,
        loanSubEvents.getTransfers.asScala))
    })

    val deposits = afterLoanActions.map(_._1).flatMap(deposits => deposits)
    val repays = afterLoanActions.map(_._2).flatMap(repays => repays)
    val transfers = afterLoanActions.map(_._3).flatMap(transfers => transfers)

    log.info(s"count of deposits in loan: ${deposits.count()}")
    log.info(s"count of repays in loan: ${repays.count()}")
    log.info(s"count of transfers in loan: ${transfers.count()}")
    (deposits, repays, transfers)
  }

}
