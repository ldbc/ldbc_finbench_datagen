package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.generation.DatagenParams
import ldbc.finbench.datagen.generation.events._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import java.util
import scala.collection.JavaConverters._
import scala.collection.SortedMap

class ActivityGenerator() extends Serializable {
  // TODO: Move the type definitions to the some common place
  type EitherPersonOrCompany = Either[Person, Company]
  type EitherPersonInvestOrCompanyInvest = Either[PersonInvestCompany, CompanyInvestCompany]

  val blockSize: Int = DatagenParams.blockSize
  // Use a common account generator to maintain the degree distribution of the generators
  val accountGenerator = new AccountGenerator()
  val loanGenerator = new LoanGenerator()
  val subEventsGenerator = new SubEvents()

  def personRegisterEvent(personRDD: RDD[Person]): RDD[PersonOwnAccount] = {
    val blocks = personRDD.zipWithUniqueId().map { case (v, k) => (k / blockSize, (k, v)) }

    val personOwnAccount = blocks.combineByKeyWithClassTag(
      personByRank => SortedMap(personByRank),
      (map: SortedMap[Long, Person], personByRank) => map + personByRank,
      (a: SortedMap[Long, Person], b: SortedMap[Long, Person]) => a ++ b
    )
      .mapPartitions(groups => {
        val personRegisterGen = new PersonRegisterEvent

        val personRegisterGroups = for {(block, persons) <- groups} yield {
          val personList = new util.ArrayList[Person](persons.size)
          for (p <- persons.values) {
            personList.add(p)
          }

          personRegisterGen.personRegister(personList, accountGenerator, block.toInt)
        }

        for {
          personOwnAccounts <- personRegisterGroups
          personOwnAccount <- personOwnAccounts.iterator().asScala
        } yield personOwnAccount
      })
    personOwnAccount
  }

  def companyRegisterEvent(companyRDD: RDD[Company]): RDD[CompanyOwnAccount] = {
    val blocks = companyRDD
      .zipWithUniqueId()
      .map(row => (row._2, row._1))
      .map { case (k, v) => (k / blockSize, (k, v)) }

    val companyOwnAccount =
      blocks
        .combineByKeyWithClassTag(
          companyById => SortedMap(companyById),
          (map: SortedMap[Long, Company], companyById) => map + companyById,
          (a: SortedMap[Long, Company], b: SortedMap[Long, Company]) => a ++ b
        )
        .mapPartitions(groups => {
          val companyRegisterGen = new CompanyRegisterEvent

          val companyRegisterGroups = for {(block, companies) <- groups} yield {
            val companyList = new util.ArrayList[Company](companies.size)
            for (p <- companies.values) {
              companyList.add(p)
            }
            companyRegisterGen.companyRegister(companyList, accountGenerator, block.toInt)
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
    val sampleRandom = new scala.util.Random(DatagenParams.defaultSeed)
    val personInvestGenerator = new PersonInvestEvent()
    val companyInvestGenerator = new CompanyInvestEvent()

    // Sample some companies to be invested
    val investedCompanyRDD = companyRDD.sample(withReplacement = false, DatagenParams.companyInvestedFraction, DatagenParams.defaultSeed)
    // Merge to either
    val personEitherRDD: RDD[EitherPersonOrCompany] = personRDD.map(person => Left(person))
    val companyEitherRDD: RDD[EitherPersonOrCompany] = companyRDD.map(company => Right(company))
    val mergedEither = personEitherRDD.union(companyEitherRDD).collect().toList

    // TODO: optimize the Spark process when large scale
    investedCompanyRDD.map(investedCompany => {
      numInvestorsRandom.setSeed(seedRandom.nextInt())
      sampleRandom.setSeed(seedRandom.nextInt())
      personInvestGenerator.resetState(seedRandom.nextInt())
      companyInvestGenerator.resetState(seedRandom.nextInt())

      val numInvestors = numInvestorsRandom.nextInt(DatagenParams.maxInvestors - DatagenParams.minInvestors + 1) + DatagenParams.minInvestors
      // Note: check if fraction 0.1 has enough numInvestors to take
      val investRels = sampleRandom.shuffle(mergedEither).take(numInvestors).map {
        case Left(person) => Left(personInvestGenerator.personInvest(person, investedCompany))
        case Right(company) => Right(companyInvestGenerator.companyInvest(company, investedCompany))
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

  def workInEvent(personRDD: RDD[Person], companyRDD: RDD[Company]): RDD[WorkIn] = {
    val fraction = DatagenParams.companyHasWorkerFraction
    val personParts = personRDD.partitions.length
    val companySampleList = new util.ArrayList[util.List[Company]](personParts)
    for (i <- 1 to personParts) {
      // TODO: consider sample
      companySampleList.add(companyRDD.collect().toList.asJava)
    }

    val personWorkInRels = personRDD.mapPartitions(persons => {
      val personList = new util.ArrayList[Person]()
      persons.foreach(personList.add)
      val personWorkInGenerator = new WorkInEvent()
      val part = TaskContext.getPartitionId()
      val personWorkInList = personWorkInGenerator.workIn(personList, companySampleList.get(part), part)
      for {
        personWorkIn <- personWorkInList.iterator().asScala
      } yield personWorkIn
    })

    personWorkInRels
  }

  def signInEvent(mediumRDD: RDD[Medium], accountRDD: RDD[Account]): RDD[SignIn] = {
    val fraction = DatagenParams.accountSignedInFraction
    val mediumParts = mediumRDD.partitions.length
    val accountSampleList = new util.ArrayList[util.List[Account]](mediumParts)
    for (i <- 1 to mediumParts) {
      // TODO: consider sample
      accountSampleList.add(accountRDD.collect().toList.asJava)
    }

    val signRels = mediumRDD.mapPartitions(mediums => {
      val mediumList = new util.ArrayList[Medium]()
      mediums.foreach(mediumList.add)
      val signGenerator = new SignInEvent()
      val part = TaskContext.getPartitionId()
      val signInList = signGenerator.signIn(mediumList, accountSampleList.get(part), part)
      for {
        signIn <- signInList.iterator().asScala
      } yield signIn
    })

    signRels
  }

  def personGuaranteeEvent(personRDD: RDD[Person]): RDD[PersonGuaranteePerson] = {
    personRDD.mapPartitions(persons => {
      val personGuaranteeEvent = new PersonGuaranteeEvent
      val personList = new util.ArrayList[Person]()
      persons.foreach(personList.add)
      val guaranteeList = personGuaranteeEvent.personGuarantee(personList, TaskContext.getPartitionId())
      for {
        guarantee <- guaranteeList.iterator().asScala
      } yield guarantee
    })
  }

  def companyGuaranteeEvent(companyRDD: RDD[Company]): RDD[CompanyGuaranteeCompany] = {
    companyRDD.mapPartitions(companies => {
      val companyGuaranteeEvent = new CompanyGuaranteeEvent
      val companyList = new util.ArrayList[Company]()
      companies.foreach(companyList.add)
      val guaranteeList = companyGuaranteeEvent.companyGuarantee(companyList, TaskContext.getPartitionId())
      for {
        guarantee <- guaranteeList.iterator().asScala
      } yield guarantee
    })
  }

  def personLoanEvent(personRDD: RDD[Person]): RDD[PersonApplyLoan] = {
    personRDD.mapPartitions(persons => {
      val personLoanEvent = new PersonLoanEvent
      val personList = new util.ArrayList[Person]()
      persons.foreach(personList.add)
      val loanList = personLoanEvent.personLoan(personList, loanGenerator, TaskContext.getPartitionId())
      for {applyLoan <- loanList.iterator().asScala} yield applyLoan
    })
  }

  def companyLoanEvent(companyRDD: RDD[Company]): RDD[CompanyApplyLoan] = {
    companyRDD.mapPartitions(companies => {
      val companyLoanEvent = new CompanyLoanEvent
      val companyList = new util.ArrayList[Company]()
      companies.foreach(companyList.add)
      val loanList = companyLoanEvent.companyLoan(companyList, loanGenerator, TaskContext.getPartitionId())
      for {
        applyLoan <- loanList.iterator().asScala
      } yield applyLoan
    })
  }

  def transferEvent(accountRDD: RDD[Account]): RDD[Transfer] = {
    accountRDD.mapPartitions(accounts => {
      val transferEvent = new TransferEvent
      val accountList = new util.ArrayList[Account]()
      accounts.foreach(accountList.add)
      val transferList = transferEvent.transfer(accountList, TaskContext.getPartitionId())
      for {transfer <- transferList.iterator().asScala} yield transfer
    })
  }

  def withdrawEvent(accountRDD: RDD[Account]): RDD[Withdraw] = {
    accountRDD.mapPartitions(accounts => {
      val withdrawEvent = new WithdrawEvent
      val accountList = new util.ArrayList[Account]()
      accounts.foreach(accountList.add)
      val withdrawList = withdrawEvent.withdraw(accountList, TaskContext.getPartitionId())
      for {withdraw <- withdrawList.iterator().asScala} yield withdraw
    })
  }

  def depositEvent(loanRDD: RDD[Loan], accountRDD: RDD[Account]): RDD[Deposit] = {
    val fraction = 1.0 // todo config the company list size

    val loanParts = loanRDD.partitions.length
    val accountSampleList = new util.ArrayList[util.List[Account]](loanParts)
    for (i <- 1 to loanParts) {
      // TODO: consider sample
      accountSampleList.add(accountRDD.collect().toList.asJava)
    }

    val depositRels = loanRDD.mapPartitions(loans => {
      val loanList = new util.ArrayList[Loan]()
      loans.foreach(loanList.add)
      val partitionId = TaskContext.getPartitionId()
      val depositList = subEventsGenerator.subEventDeposit(loanList, accountSampleList.get(partitionId), partitionId)

      for {
        deposit <- depositList.iterator().asScala
      } yield deposit
    })
    depositRels
  }

  def repayEvent(accountRDD: RDD[Account], loanRDD: RDD[Loan]): RDD[Repay] = {
    val fraction = 1.0 // todo config the company list size
    val accountParts = accountRDD.partitions.length
    val loanSampleList = new util.ArrayList[util.List[Loan]](accountParts)
    for (i <- 1 to accountParts) {
      // TODO: consider sample
      loanSampleList.add(loanRDD.collect().toList.asJava)
    }

    val repayRels = accountRDD.mapPartitions(accounts => {
      val accountList = new util.ArrayList[Account]()
      accounts.foreach(accountList.add)
      val partitionId = TaskContext.getPartitionId()
      val repayList = subEventsGenerator.subEventRepay(accountList, loanSampleList.get(partitionId), partitionId)
      for {
        repay <- repayList.iterator().asScala
      } yield repay
    })

    repayRels
  }

}
