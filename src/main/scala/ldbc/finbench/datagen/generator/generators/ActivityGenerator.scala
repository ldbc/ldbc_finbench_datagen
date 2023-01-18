package ldbc.finbench.datagen.generator.generators

import ldbc.finbench.datagen.entities.nodes.{Account, Company, Medium, Person}
import ldbc.finbench.datagen.generator.DatagenParams
import ldbc.finbench.datagen.generator.events.{
  CompanyGuaranteeEvent,
  CompanyInvestEvent,
  CompanyLoanEvent,
  CompanyRegisterEvent,
  PersonGuaranteeEvent,
  PersonInvestEvent,
  PersonLoanEvent,
  PersonRegisterEvent,
  SignInEvent,
  TransferEvent,
  WithdrawEvent,
  WorkInEvent
}
import org.apache.spark.rdd.RDD
import java.util

import ldbc.finbench.datagen.entities.edges.{
  CompanyApplyLoan,
  CompanyGuaranteeCompany,
  CompanyInvestCompany,
  CompanyOwnAccount,
  PersonApplyLoan,
  PersonGuaranteePerson,
  PersonInvestCompany,
  PersonOwnAccount,
  SignIn,
  Transfer,
  Withdraw,
  WorkIn
}
import ldbc.finbench.datagen.model.raw.CompanyApplyLoanRaw
import ldbc.finbench.datagen.util.GeneratorConfiguration
import org.apache.spark.TaskContext

import scala.collection.JavaConverters._
import scala.collection.SortedMap
import scala.util.Random

class ActivityGenerator(conf: GeneratorConfiguration) {
  val blockSize = DatagenParams.blockSize

  def personRegisterEvent(personRDD: RDD[Person]): RDD[PersonOwnAccount] = {
    val blocks = personRDD.zipWithUniqueId().map { case (v, k) => (k / blockSize, (k, v)) }

    val personOwnAccount = blocks
      .combineByKeyWithClassTag(
        personByRank => SortedMap(personByRank),
        (map: SortedMap[Long, Person], personByRank) => map + personByRank,
        (a: SortedMap[Long, Person], b: SortedMap[Long, Person]) => a ++ b
      )
      .mapPartitions(groups => {
        val personRegisterGeneratorClass = Class.forName("PersonRegisterEvent")
        val personRegisterGen = personRegisterGeneratorClass
          .getConstructor()
          .newInstance()
          .asInstanceOf[PersonRegisterEvent]

        val personRegisterGroups = for { (block, persons) <- groups } yield {
          val personList = new util.ArrayList[Person](persons.size)
          for (p <- persons.values) { personList.add(p) }

          personRegisterGen.personRegister(personList, block.toInt, conf)
        }

        //todo return the personOwnAccount relationship and account vertices
        for {
          personOwnAccounts <- personRegisterGroups
          personOwnAccount  <- personOwnAccounts.iterator().asScala
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
          val companyRegisterGeneratorClass = Class.forName("CompanyRegisterEvent")
          val companyRegisterGen = companyRegisterGeneratorClass
            .getConstructor()
            .newInstance()
            .asInstanceOf[CompanyRegisterEvent]

          val companyRegisterGroups = for { (block, companies) <- groups } yield {
            val companyList = new util.ArrayList[Company](companies.size)
            for (p <- companies.values) { companyList.add(p) }
            companyRegisterGen.companyRegister(companyList, block.toInt, conf)
          }

          //todo return the companyOwnAccount relationship and account vertices
          for {
            companyOwnAccounts <- companyRegisterGroups
            companyOwnAccount  <- companyOwnAccounts.iterator().asScala
          } yield companyOwnAccount
        })
    companyOwnAccount
  }

  def personInvestEvent(personRDD: RDD[Person],
                        companyRDD: RDD[Company]): RDD[PersonInvestCompany] = {

    val companyListSize   = 5000 // todo config the company list size
    val randomCompanySize = 5000 / companyRDD.partitions.length

    val personInvestRels = personRDD.mapPartitions(persons => {
      val personList = new util.ArrayList[Person]()
      if (persons.hasNext) {
        personList.add(persons.next())
      }

      val companies = companyRDD
        .mapPartitions(coms => {
          val companyList = new util.ArrayList[Company]()
          val random      = new Random()
          val comsArray   = coms.toArray
          while (companyList.size() < randomCompanySize) {
            companyList.add(comsArray(random.nextInt(randomCompanySize)))
          }
          companyList.asScala.toIterator
        })
        .collect()
        .toList
        .asJava

      val personInvestGenerator = new PersonInvestEvent()

      // todo return invest relationship
      val personInvestList =
        personInvestGenerator.personInvest(personList, companies, TaskContext.get.partitionId)

      for {
        personInvest <- personInvestList.iterator().asScala
      } yield personInvest
    })

    personInvestRels
  }

  def companyInvestEvent(companyRDD: RDD[Company]): RDD[CompanyInvestCompany] = {

    val companyListSize   = 5000 // todo config the company list size
    val randomCompanySize = 5000 / companyRDD.partitions.length

    val companyInvestRels = companyRDD.mapPartitions(companies => {
      val companyList = new util.ArrayList[Company]()
      if (companies.hasNext) {
        companyList.add(companies.next())
      }

      val investCompanies = companyRDD
        .mapPartitions(coms => {
          val companyList = new util.ArrayList[Company]()
          val random      = new Random()
          val comsArray   = coms.toArray
          while (companyList.size() < randomCompanySize) {
            companyList.add(comsArray(random.nextInt(randomCompanySize)))
          }
          companyList.asScala.toIterator
        })
        .collect()
        .toList
        .asJava

      val companyInvestGenerator = new CompanyInvestEvent()

      // todo return invest relationship
      val companyInvestList =
        companyInvestGenerator.companyInvest(companyList, TaskContext.get.partitionId)

      for {
        companyInvest <- companyInvestList.iterator().asScala
      } yield companyInvest
    })

    companyInvestRels
  }

  def workInEvent(personRDD: RDD[Person], companyRDD: RDD[Company]): RDD[WorkIn] = {
    val companyListSize   = 5000 // todo config the company list size
    val randomCompanySize = 5000 / companyRDD.partitions.length

    val personWorkInRels = personRDD.mapPartitions(persons => {
      val personList = new util.ArrayList[Person]()
      if (persons.hasNext) {
        personList.add(persons.next())
      }

      val companies = companyRDD
        .mapPartitions(coms => {
          val companyList = new util.ArrayList[Company]()
          val random      = new Random()
          val comsArray   = coms.toArray
          while (companyList.size() < randomCompanySize) {
            companyList.add(comsArray(random.nextInt(randomCompanySize)))
          }
          companyList.asScala.toIterator
        })
        .collect()
        .toList
        .asJava

      val personWorkInGenerator = new WorkInEvent()

      // todo return invest relationship
      val personWorkInList =
        personWorkInGenerator.workIn(personList, companies, TaskContext.get.partitionId)

      for {
        personWorkIn <- personWorkInList.iterator().asScala
      } yield personWorkIn
    })

    personWorkInRels
  }

  def signEvent(mediumRDD: RDD[Medium], accountRDD: RDD[Account]): RDD[SignIn] = {
    val accountListSize   = 5000 // todo config the company list size
    val randomAccountSize = accountListSize / accountRDD.partitions.length

    val signRels = mediumRDD.mapPartitions(mediums => {
      val mediumList = new util.ArrayList[Medium]()
      if (mediums.hasNext) {
        mediumList.add(mediums.next())
      }

      val accounts = accountRDD
        .mapPartitions(accounts => {
          val accountList = new util.ArrayList[Account]()
          val random      = new Random()
          val comsArray   = accounts.toArray
          while (accountList.size() < randomAccountSize) {
            accountList.add(comsArray(random.nextInt(randomAccountSize)))
          }
          accountList.asScala.toIterator
        })
        .collect()
        .toList
        .asJava

      val signGenerator = new SignInEvent()

      // todo return invest relationship
      val signInList =
        signGenerator.signIn(mediumList, accounts, TaskContext.get.partitionId, conf)

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
      if (persons.hasNext) {
        personList.add(persons.next())
      }
      val guaranteeList =
        personGuaranteeEvent.personGuarantee(personList, TaskContext.getPartitionId())
      for {
        guarantee <- guaranteeList.iterator().asScala
      } yield guarantee
    })
  }

  def companyGuaranteeEvent(companyRDD: RDD[Company]): RDD[CompanyGuaranteeCompany] = {
    companyRDD.mapPartitions(companies => {
      val companyGuaranteeEvent = new CompanyGuaranteeEvent

      val companyList = new util.ArrayList[Company]()
      if (companies.hasNext) {
        companyList.add(companies.next())
      }
      val guaranteeList =
        companyGuaranteeEvent.companyGuarantee(companyList, TaskContext.getPartitionId())
      for {
        guarantee <- guaranteeList.iterator().asScala
      } yield guarantee
    })
  }

  def personLoanEvent(personRDD: RDD[Person]): RDD[PersonApplyLoan] = {
    personRDD.mapPartitions(persons => {
      val personLoanEvent = new PersonLoanEvent
      val personList      = new util.ArrayList[Person]()
      if (persons.hasNext) {
        personList.add(persons.next())
      }
      val loanList = personLoanEvent.personLoan(personList, TaskContext.getPartitionId(), conf)
      for { applyLoan <- loanList.iterator().asScala } yield applyLoan
    })
  }

  def companyLoanEvent(companyRDD: RDD[Company]): RDD[CompanyApplyLoan] = {
    companyRDD.mapPartitions(companies => {
      val companyLoanEvent = new CompanyLoanEvent

      val companyList = new util.ArrayList[Company]()
      if (companies.hasNext) {
        companyList.add(companies.next())
      }
      val loanList =
        companyLoanEvent.companyLoan(companyList, TaskContext.getPartitionId(), conf)
      for {
        applyLoan <- loanList.iterator().asScala
      } yield applyLoan
    })
  }

  def transferEvent(accountRDD: RDD[Account]): RDD[Transfer] = {
    accountRDD.mapPartitions(accounts => {
      val transferEvent = new TransferEvent
      val accountList   = new util.ArrayList[Account]()
      if (accounts.hasNext) {
        accountList.add(accounts.next())
      }
      val transferList = transferEvent.transfer(accountList, TaskContext.getPartitionId())
      for { transfer <- transferList.iterator().asScala } yield transfer
    })
  }

  def withdrawEvent(accountRDD: RDD[Account]): RDD[Withdraw] = {
    accountRDD.mapPartitions(accounts => {
      val withdrawEvent = new WithdrawEvent
      val accountList   = new util.ArrayList[Account]()
      if (accounts.hasNext) {
        accountList.add(accounts.next())
      }
      val withdrawList = withdrawEvent.withdraw(accountList, TaskContext.getPartitionId())
      for { withdraw <- withdrawList.iterator().asScala } yield withdraw
    })
  }

}
