package ldbc.finbench.datagen.generation

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
//  - refactor using common GraphDef to make the code less verbose
//  - repartition with the partition option
class ActivitySimulator(sink: RawSink)(implicit spark: SparkSession) extends Writer[RawSink] with Serializable with Logging {

  private val options: Map[String, String] = sink.formatOptions ++ Map("header" -> "true", "delimiter" -> "|")
  private val parallelism = spark.sparkContext.defaultParallelism
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
    val personRdd: RDD[Person] = SparkPersonGenerator(personNum, blockSize, personPartitions)
    val companyRdd: RDD[Company] = SparkCompanyGenerator(companyNum, blockSize, companyPartitions)
    val mediumRdd: RDD[Medium] = SparkMediumGenerator(mediumNum, blockSize, mediumPartitions)
    log.info(s"[Simulation] Person RDD partitions: ${personRdd.getNumPartitions}")
    log.info(s"[Simulation] Company RDD partitions: ${companyRdd.getNumPartitions}")
    log.info(s"[Simulation] Medium RDD partitions: ${mediumRdd.getNumPartitions}")

    // simulate person and company register account event
    val personWithAccounts = activityGenerator.personRegisterEvent(personRdd)
    val companyWithAccounts = activityGenerator.companyRegisterEvent(companyRdd)
    val accountRdd = mergeAccounts(personWithAccounts, companyWithAccounts) // merge
    log.info(s"[Simulation] Account RDD partitions: ${accountRdd.getNumPartitions}")

    // simulate person signIn medium event
    val signInRdd = activityGenerator.signInEvent(mediumRdd, accountRdd)
    log.info(s"[Simulation] signIn RDD partitions: ${signInRdd.getNumPartitions}")

    // simulate person or company invest company event
    val investRdd = activityGenerator.investEvent(personRdd, companyRdd)
    log.info(s"[Simulation] invest RDD partitions: ${investRdd.getNumPartitions}")

    // simulate person guarantee person event and company guarantee company event
    val personWithAccGua = activityGenerator.personGuaranteeEvent(personWithAccounts)
    val companyWitAccGua = activityGenerator.companyGuaranteeEvent(companyWithAccounts)
    log.info(s"[Simulation] personGuarantee RDD partitions: ${personWithAccGua.getNumPartitions}, " +
      s"companyGuarantee RDD partitions: ${companyWitAccGua.getNumPartitions}")

    // simulate person apply loans event and company apply loans event
    val personLoanRdd = activityGenerator.personLoanEvent(personWithAccGua)
    val companyLoanRdd = activityGenerator.companyLoanEvent(companyWitAccGua).cache()
    log.info(s"[Simulation] personApplyLoan RDD partitions: ${personLoanRdd.getNumPartitions}, " +
      s"companyApplyLoan RDD partitions: ${companyLoanRdd.getNumPartitions}")

    // Merge accounts vertices registered by persons and companies
    val personLoans = personLoanRdd.map(personLoan => personLoan.getLoan)
    assert(personLoans.count() == personLoanRdd.count())
    // Don't why the loanRdd lost values if don't cache companyLoans
    val companyLoans = companyLoanRdd.map(companyLoan => companyLoan.getLoan).cache()
    assert(companyLoans.count() == companyLoanRdd.count())

    // simulate loan subevents including deposit, repay and transfer
    val loanRdd = personLoans.union(companyLoans)
    assert((personLoans.count() + companyLoans.count()) == loanRdd.count())
    val (depositsRdd, repaysRdd, loanTrasfersRdd) = activityGenerator.afterLoanSubEvents(loanRdd, accountRdd)
    log.info(s"[Simulation] Loan RDD partitions: ${loanRdd.getNumPartitions}")
    log.info(s"[Simulation] deposits RDD partitions: ${depositsRdd.getNumPartitions}, " +
      s"repays RDD partitions: ${repaysRdd.getNumPartitions}, " +
      s"loanTrasfers RDD partitions: ${loanTrasfersRdd.getNumPartitions}")

    // simulate transfer and withdraw event
    val transferRdd = activityGenerator.transferEvent(accountRdd)
    val withdrawRdd = activityGenerator.withdrawEvent(accountRdd)
    log.info(s"[Simulation] transfer RDD partitions: ${transferRdd.getNumPartitions}, " +
      s"withdraw RDD partitions: ${withdrawRdd.getNumPartitions}")

    activitySerializer.writePersonWithActivities(personWithAccGua)
    activitySerializer.writeCompanyWithActivities(companyWitAccGua)
    activitySerializer.writeMediumWithActivities(mediumRdd, signInRdd)
    activitySerializer.writeAccount(accountRdd)
    activitySerializer.writeInvest(investRdd)
    activitySerializer.writeSignIn(signInRdd)
    activitySerializer.writePersonLoan(personLoanRdd)
    activitySerializer.writeCompanyLoan(companyLoanRdd)
    activitySerializer.writeLoanActivities(loanRdd, depositsRdd, repaysRdd, loanTrasfersRdd)
    activitySerializer.writeTransfer(transferRdd)
    activitySerializer.writeWithdraw(withdrawRdd)
  }

  private def mergeAccounts(persons: RDD[Person], companies: RDD[Company]): RDD[Account] = {
    val personAccounts = persons.flatMap(person => person.getPersonOwnAccounts.asScala.map(_.getAccount))
    val companyAccounts = companies.flatMap(company => company.getCompanyOwnAccounts.asScala.map(_.getAccount))
    personAccounts.union(companyAccounts)
  }
}
