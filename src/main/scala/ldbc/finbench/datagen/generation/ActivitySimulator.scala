package ldbc.finbench.datagen.generation

import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.generation.generators.{ActivityGenerator, SparkCompanyGenerator, SparkMediumGenerator, SparkPersonGenerator}
import ldbc.finbench.datagen.generation.serializers.ActivitySerializer
import ldbc.finbench.datagen.io.Writer
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.util.GeneratorConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// TODO:
//  - re-implement the code in more elegant and less verbose way
//  - repartition with the partition option
//  - config the paramMap (including header, mode, dateFormat and so on)
class ActivitySimulator(sink: RawSink)(implicit spark: SparkSession) extends Writer[RawSink] with Serializable {

  private val options: Map[String, String] = sink.formatOptions ++ Map("header" -> "true", "delimiter" -> "|")
  private val parallelism = spark.sparkContext.defaultParallelism // TODO: compute parallelism
  private val blockSize: Int = DatagenParams.blockSize

  private val activityGenerator = new ActivityGenerator()
  private val activitySerializer = new ActivitySerializer(sink, options)

  private val personNum: Long = DatagenParams.numPersons
  private val personPartitions = Some(Math.min(Math.ceil(personNum.toDouble / blockSize).toLong, parallelism).toInt)
  private val personRdd: RDD[Person] = SparkPersonGenerator(personNum, blockSize, personPartitions)

  private val companyNum: Long = DatagenParams.numCompanies
  private val companyPartitions = Some(Math.min(Math.ceil(companyNum.toDouble / blockSize).toLong, parallelism).toInt)
  private val companyRdd: RDD[Company] = SparkCompanyGenerator(companyNum, blockSize, companyPartitions)

  private val mediumNum: Long = DatagenParams.numMediums
  private val mediumPartitions = Some(Math.min(Math.ceil(mediumNum.toDouble / blockSize).toLong, parallelism).toInt)
  private val mediumRdd: RDD[Medium] = SparkMediumGenerator(mediumNum, blockSize, mediumPartitions)

  def simulate(): Unit = {
    // simulate person register account event
    val personOwnAccountInfo = activityGenerator.personRegisterEvent(personRdd)

    // simulate company register account event
    val companyOwnAccountInfo = activityGenerator.companyRegisterEvent(companyRdd)

    // Merge accounts vertices registered by persons and companies
    // TODO: can not coalesce when large scale data generated in cluster
    val accountRdd = personOwnAccountInfo.map(personOwnAccount => personOwnAccount.getAccount)
      .union(companyOwnAccountInfo.map(companyOwnAccount => companyOwnAccount.getAccount))
      .coalesce(1)

    // simulate person or company invest company event
    val investRdd = activityGenerator.investEvent(personRdd, companyRdd)

    // simulate person work in company event
    val workInRdd = activityGenerator.workInEvent(personRdd, companyRdd)

    // simulate person signIn medium event
    val signInRdd = activityGenerator.signInEvent(mediumRdd, accountRdd)

    // simulate person guarantee person event
    val personGuaranteeRdd = activityGenerator.personGuaranteeEvent(personRdd)

    // simulate company guarantee company event
    val companyGuaranteeRdd = activityGenerator.companyGuaranteeEvent(companyRdd)

    // simulate person apply loans event
    val personLoanRdd = activityGenerator.personLoanEvent(personRdd)

    // simulate company apply loans event
    val companyLoanRdd = activityGenerator.companyLoanEvent(companyRdd)

    // Merge accounts vertices registered by persons and companies
    val loanRdd = personLoanRdd.map(personLoan => personLoan.getLoan)
      .union(companyLoanRdd.map(companyLoan => companyLoan.getLoan))
      .coalesce(1)

    // simulate transfer event
    val transferRdd = activityGenerator.transferEvent(accountRdd)

    // simulate withdraw event
    val withdrawRdd = activityGenerator.withdrawEvent(accountRdd)

    // simulate deposit event
    val depositRdd = activityGenerator.depositEvent(loanRdd, accountRdd)

    // simulate repay event
    val repayRdd = activityGenerator.repayEvent(accountRdd, loanRdd)

    // TODO: use some syntax to implement serializer less verbose like GraphDef
    activitySerializer.writePerson(personRdd)
    activitySerializer.writeCompany(companyRdd)
    activitySerializer.writeMedium(mediumRdd)
    activitySerializer.writePersonOwnAccount(personOwnAccountInfo)
    activitySerializer.writeCompanyOwnAccount(companyOwnAccountInfo)
    activitySerializer.writeAccount(accountRdd)
    activitySerializer.writeInvest(investRdd)
    activitySerializer.writeWorkIn(workInRdd)
    activitySerializer.writeSignIn(signInRdd)
    activitySerializer.writePersonGuarantee(personGuaranteeRdd)
    activitySerializer.writeCompanyGuarantee(companyGuaranteeRdd)
    activitySerializer.writePersonLoan(personLoanRdd)
    activitySerializer.writeCompanyLoan(companyLoanRdd)
    activitySerializer.writeLoan(loanRdd)
    activitySerializer.writeTransfer(transferRdd)
    activitySerializer.writeWithdraw(withdrawRdd)
    activitySerializer.writeDeposit(depositRdd)
    activitySerializer.writeRepay(repayRdd)
  }
}
