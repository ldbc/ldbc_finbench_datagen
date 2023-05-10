package ldbc.finbench.datagen.generator.serializers

import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.generator.generators.ActivityGenerator
import ldbc.finbench.datagen.io.Writer
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.util.GeneratorConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// TODO:
//  - re-implement the code in more elegant way
//  - repartition with the partition option
//  - config the paramMap (including header, mode, dateFormat and so on)
//  - replace the random with java entities properties
class RawSerializer(sink: RawSink, conf: GeneratorConfiguration)(implicit spark: SparkSession) extends Writer[RawSink] with Serializable {

  val options = sink.formatOptions ++ Map("header" -> "true", "delimiter" -> "|")
  val activityGenerator = new ActivityGenerator(conf)
  val activitySerializer = new ActivitySerializer(sink, options)

  def write(personRdd: RDD[Person], companyRdd: RDD[Company], mediumRdd: RDD[Medium]): Unit = {
    activitySerializer.writePerson(personRdd)
    activitySerializer.writeCompany(companyRdd)
    activitySerializer.writeMedium(mediumRdd)
    writeEvent(personRdd, companyRdd, mediumRdd)
  }

  private def writeEvent(personRdd: RDD[Person], companyRdd: RDD[Company], mediumRdd: RDD[Medium]): Unit = {
    // Simulation: simulate person register account event
    val personOwnAccountInfo = activityGenerator.personRegisterEvent(personRdd)

    // Simulation: simulate company register account event
    val companyOwnAccountInfo = activityGenerator.companyRegisterEvent(companyRdd)

    // Merge accounts vertices registered by persons and companies
    val accountInfoByPerson = personOwnAccountInfo.map(personOwnAccountRaw => {
      personOwnAccountRaw.getAccount
    })
    val accountInfoByCompany = companyOwnAccountInfo.map(companyOwnAccountRaw => {
      companyOwnAccountRaw.getAccount
    })
    val accountRdd = accountInfoByPerson.union(accountInfoByCompany)

    // Simulation: simulate person invest company event
    val personInvestRdd = activityGenerator.personInvestEvent(personRdd, companyRdd)

    // Simulation: simulate company invest company event
    val companyInvestRdd = activityGenerator.companyInvestEvent(companyRdd)

    // Simulation: simulate person work in company event
    val workInRdd = activityGenerator.workInEvent(personRdd, companyRdd)

    // Simulation: simulate person signIn medium event
    val signInRdd = activityGenerator.signInEvent(mediumRdd, accountRdd)

    // Simulation: simulate person guarantee person event
    val personGuaranteeRdd = activityGenerator.personGuaranteeEvent(personRdd)

    // Simulation: simulate company guarantee company event
    val companyGuaranteeRdd = activityGenerator.companyGuaranteeEvent(companyRdd)

    // Simulation: simulate person apply loans event
    val personLoanRdd = activityGenerator.personLoanEvent(personRdd)

    // Simulation: simulate company apply loans event
    val companyLoanRdd = activityGenerator.companyLoanEvent(companyRdd)

    // Merge accounts vertices registered by persons and companies
    val loanInfoAppliedByPerson = personLoanRdd.map(personApplyLoan => {
      new Loan(personApplyLoan.getLoan.getLoanId, personApplyLoan.getLoan.getLoanAmount, personApplyLoan.getLoan.getBalance, personApplyLoan.getCreationDate, 10)
    })
    val loanInfoAppliedByCompany = companyLoanRdd.map(companyApplyLoan => {
      new Loan(companyApplyLoan.getLoan.getLoanId, companyApplyLoan.getLoanAmount, companyApplyLoan.getLoanBalance, companyApplyLoan.getCreationDate, 10)
    })
    val loanRdd = loanInfoAppliedByPerson.union(loanInfoAppliedByCompany)

    // Simulation: simulate transfer event
    val transferRdd = activityGenerator.transferEvent(accountRdd)

    // Simulation: simulate withdraw event
    val withdrawRdd = activityGenerator.withdrawEvent(accountRdd)

    // Simulation: simulate deposit event
    val depositRdd = activityGenerator.depositEvent(loanRdd, accountRdd)

    // Simulation: simulate repay event
    val repayRdd = activityGenerator.repayEvent(accountRdd, loanRdd)

    activitySerializer.writePersonOwnAccount(personOwnAccountInfo)
    activitySerializer.writeCompanyOwnAccount(companyOwnAccountInfo)
    activitySerializer.writeAccount(accountRdd)
    activitySerializer.writePersonInvest(personInvestRdd)
    activitySerializer.writeCompanyInvest(companyInvestRdd)
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