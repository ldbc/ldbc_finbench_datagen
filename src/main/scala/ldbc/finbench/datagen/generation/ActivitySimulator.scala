package ldbc.finbench.datagen.generation

import ldbc.finbench.datagen.entities.edges.{CompanyApplyLoan, PersonApplyLoan}
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.generation.generators.{ActivityGenerator, SparkCompanyGenerator, SparkMediumGenerator, SparkPersonGenerator}
import ldbc.finbench.datagen.generation.serializers.ActivitySerializer
import ldbc.finbench.datagen.io.Writer
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

// TODO:
//  - re-implement the code in more elegant and less verbose way
//  - repartition with the partition option
//  - config the paramMap (including header, mode, dateFormat and so on)
class ActivitySimulator(sink: RawSink)(implicit spark: SparkSession) extends Writer[RawSink] with Serializable with Logging {

  private val options: Map[String, String] = sink.formatOptions ++ Map("header" -> "true", "delimiter" -> "|")
  private val parallelism = spark.sparkContext.defaultParallelism // TODO: compute parallelism
  private val blockSize: Int = DatagenParams.blockSize

  private val activityGenerator = new ActivityGenerator()
  private val activitySerializer = new ActivitySerializer(sink, options)

  private val personNum: Long = DatagenParams.numPersons
  private val personPartitions = Some(Math.min(Math.ceil(personNum.toDouble / blockSize).toLong, parallelism).toInt)

  private val companyNum: Long = DatagenParams.numCompanies
  private val companyPartitions = Some(Math.min(Math.ceil(companyNum.toDouble / blockSize).toLong, parallelism).toInt)

  private val mediumNum: Long = DatagenParams.numMediums
  private val mediumPartitions = Some(Math.min(Math.ceil(mediumNum.toDouble / blockSize).toLong, parallelism).toInt)


  def simulate(): Unit = {
    // simulate person register account event
    val personRdd: RDD[Person] = SparkPersonGenerator(personNum, blockSize, personPartitions)
    val personOwnAccountInfo = activityGenerator.personRegisterEvent(personRdd)
    // Add the ownAccount info back to the person vertices
    val poas = personOwnAccountInfo.collect()
    val personWithAccountRdd = personRdd.map(person => {
      person.getPersonOwnAccounts.addAll(poas.filter(_.getPerson.equals(person)).toList.asJava)
      person
    })
    log.info(s"[Simulation] Person RDD partitions: ${personRdd.getNumPartitions}, count: ${personRdd.count()}")

    // simulate company register account event
    val companyRdd: RDD[Company] = SparkCompanyGenerator(companyNum, blockSize, companyPartitions)
    val companyOwnAccountInfo = activityGenerator.companyRegisterEvent(companyRdd)
    // Add the ownAccount info back to the company vertices
    val coas = companyOwnAccountInfo.collect()
    val companyWithAccountRdd = companyRdd.map(company => {
      company.getCompanyOwnAccounts.addAll(coas.filter(_.getCompany.equals(company)).toList.asJava)
      company
    })
    log.info(s"[Simulation] Company RDD partitions: ${companyRdd.getNumPartitions}, count: ${companyRdd.count()}")

    // Merge accounts vertices registered by persons and companies
    // TODO: can not coalesce when large scale data generated in cluster
    val personAccounts = personOwnAccountInfo.map(personOwnAccount => personOwnAccount.getAccount)
    assert(personAccounts.count() == personOwnAccountInfo.count())
    val companyAccounts = companyOwnAccountInfo.map(companyOwnAccount => companyOwnAccount.getAccount)
    assert(companyAccounts.count() == companyOwnAccountInfo.count())
    val accountRdd = personAccounts.union(companyAccounts)
    log.info(s"[Simulation] Account RDD partitions: ${accountRdd.getNumPartitions}, count: ${accountRdd.count()}")
    assert((personAccounts.count() + companyAccounts.count()) == accountRdd.count())

    // simulate person signIn medium event
    val mediumRdd: RDD[Medium] = SparkMediumGenerator(mediumNum, blockSize, mediumPartitions)
    val signInRdd = activityGenerator.signInEvent(mediumRdd, accountRdd)
    log.info(s"[Simulation] Medium RDD partitions: ${mediumRdd.getNumPartitions}, count: ${mediumRdd.count()}")
    log.info(s"[Simulation] signIn RDD partitions: ${signInRdd.getNumPartitions}, count: ${signInRdd.count()}")

    // simulate person or company invest company event
    val investRdd = activityGenerator.investEvent(personRdd, companyRdd)
    log.info(s"[Simulation] invest RDD partitions: ${investRdd.getNumPartitions}, count: ${investRdd.count()}")

    // simulate person guarantee person event and company guarantee company event
    val personGuaranteeRdd = activityGenerator.personGuaranteeEvent(personRdd)
    val companyGuaranteeRdd = activityGenerator.companyGuaranteeEvent(companyRdd)
    log.info(s"[Simulation] personGuarantee RDD partitions: ${personGuaranteeRdd.getNumPartitions}, count: ${personGuaranteeRdd.count()}")
    log.info(s"[Simulation] companyGuarantee RDD partitions: ${companyGuaranteeRdd.getNumPartitions}, count: ${companyGuaranteeRdd.count()}")

    // simulate person apply loans event and company apply loans event
    val personLoanRdd = activityGenerator.personLoanEvent(personWithAccountRdd)
    val companyLoanRdd = activityGenerator.companyLoanEvent(companyWithAccountRdd).cache()
    log.info(s"[Simulation] personApplyLoan RDD partitions: ${personLoanRdd.getNumPartitions}, count: ${personLoanRdd.count()}")
    log.info(s"[Simulation] companyApplyLoan RDD partitions: ${companyLoanRdd.getNumPartitions}, count: ${companyLoanRdd.count()}")

    // Merge accounts vertices registered by persons and companies
    val personLoans = personLoanRdd.map(personLoan => personLoan.getLoan)
    assert(personLoans.count() == personLoanRdd.count())
    // Don't why the loanRdd lost values if don't cache companyLoans
    val companyLoans = companyLoanRdd.map(companyLoan => companyLoan.getLoan).cache()
    assert(companyLoans.count() == companyLoanRdd.count())

    val loanRdd = personLoans.union(companyLoans)
    log.info(s"[Simulation] Loan RDD partitions: ${loanRdd.getNumPartitions}, count: ${loanRdd.count()}")
    assert((personLoans.count() + companyLoans.count()) == loanRdd.count())

    // simulate loan subevents including deposit, repay and transfer
    val (depositsRdd, repaysRdd, loanTrasfersRdd) = activityGenerator.afterLoanSubEvents(loanRdd, accountRdd)
    log.info(s"[Simulation] deposits RDD partitions: ${depositsRdd.getNumPartitions}, count: ${depositsRdd.count()}")
    log.info(s"[Simulation] repays RDD partitions: ${repaysRdd.getNumPartitions}, count: ${repaysRdd.count()}")
    log.info(s"[Simulation] loanTrasfers RDD partitions: ${loanTrasfersRdd.getNumPartitions}, count: ${loanTrasfersRdd.count()}")

    // simulate transfer and withdraw event
    val transferRdd = activityGenerator.transferEvent(accountRdd)
    log.info(s"[Simulation] transfer RDD partitions: ${transferRdd.getNumPartitions}, count: ${transferRdd.count()}")
    val withdrawRdd = activityGenerator.withdrawEvent(accountRdd)
    log.info(s"[Simulation] withdraw RDD partitions: ${withdrawRdd.getNumPartitions}, count: ${withdrawRdd.count()}")

    // TODO: use some syntax to implement serializer less verbose like GraphDef
    activitySerializer.writePerson(personRdd)
    activitySerializer.writeCompany(companyRdd)
    activitySerializer.writeMedium(mediumRdd)
    activitySerializer.writePersonOwnAccount(personOwnAccountInfo)
    activitySerializer.writeCompanyOwnAccount(companyOwnAccountInfo)
    activitySerializer.writeAccount(accountRdd)
    activitySerializer.writeInvest(investRdd)
    activitySerializer.writeSignIn(signInRdd)
    activitySerializer.writePersonGuarantee(personGuaranteeRdd)
    activitySerializer.writeCompanyGuarantee(companyGuaranteeRdd)
    activitySerializer.writePersonLoan(personLoanRdd)
    activitySerializer.writeCompanyLoan(companyLoanRdd)
    activitySerializer.writeLoan(loanRdd)
    activitySerializer.writeDeposit(depositsRdd)
    activitySerializer.writeRepay(repaysRdd)
    activitySerializer.writeLoanTransfer(loanTrasfersRdd)
    activitySerializer.writeTransfer(transferRdd)
    activitySerializer.writeWithdraw(withdrawRdd)
  }
}
