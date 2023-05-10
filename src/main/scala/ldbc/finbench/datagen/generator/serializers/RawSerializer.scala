package ldbc.finbench.datagen.generator.serializers

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.generator.generators.ActivityGenerator
import ldbc.finbench.datagen.io.Writer
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.model.raw._
import ldbc.finbench.datagen.util.GeneratorConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

// todo repartition with the partition option
// todo config the paramMap (including header, mode, dateFormat and so on)
// todo replace the random with java entities properties
class RawSerializer(sink: RawSink, conf: GeneratorConfiguration)(implicit spark: SparkSession)
  extends Writer[RawSink]
    with Serializable {
  val random = new Random()
  val options = sink.formatOptions ++ Map("header" -> "true", "delimiter" -> "|")

  def write(personRdd: RDD[Person], companyRdd: RDD[Company], mediumRdd: RDD[Medium]): Unit = {
    writePerson(personRdd)
    writeCompany(companyRdd)
    writeMedium(mediumRdd)
//    writeEvent(personRdd, companyRdd, mediumRdd)
  }

  private def writePerson(self: RDD[Person]): Unit = {
    val rawPersons = self.map { p: Person => PersonRaw(p.getPersonId, p.getCreationDate, p.getPersonName, p.isBlocked) }
    val df = spark.createDataFrame(rawPersons)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/person")
  }

  private def writeCompany(self: RDD[Company]): Unit = {
    val rawCompanies = self.map { c: Company => CompanyRaw(c.getCompanyId, c.getCreationDate, c.getCompanyName, c.isBlocked) }
    val df = spark.createDataFrame(rawCompanies)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/company")
  }

  private def writeMedium(self: RDD[Medium]): Unit = {
    val rawMedium = self.map { m: Medium => MediumRaw(m.getMediumId, m.getCreationDate, m.getMediumName, m.isBlocked) }
    val df = spark.createDataFrame(rawMedium)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/medium")
  }

  private def writeAccount(self: RDD[Account]): Unit = {
    val rawAccount = self.map { a: Account => AccountRaw(a.getAccountId, a.getCreationDate, a.getDeletionDate, a.isBlocked, a.getType) }
    val df = spark.createDataFrame(rawAccount)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/account")
  }

  private def writePersonOwnAccount(self: RDD[PersonOwnAccountRaw]): Unit = {
    val df = spark.createDataFrame(self)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/personOwnAccount")
  }

  private def writeCompanyOwnAccount(self: RDD[CompanyOwnAccountRaw]): Unit = {
    val df = spark.createDataFrame(self)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/companyOwnAccount")
  }

  private def writePersonInvest(self: RDD[PersonInvestCompany]): Unit = {
    val rawPersonInvest = self.map { invest =>
      PersonInvestCompanyRaw(invest.getPersonId,
        invest.getCompanyId,
        invest.getCreationDate,
        random.nextFloat())
    }
    val df = spark.createDataFrame(rawPersonInvest)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/personInvest")
  }

  private def writeCompanyInvest(self: RDD[CompanyInvestCompany]): Unit = {
    val rawCompanyInvest = self.map { invest =>
      CompanyInvestCompanyRaw(invest.getFromCompanyId,
        invest.getToCompanyId,
        invest.getCreationDate,
        random.nextFloat())
    }
    val df = spark.createDataFrame(rawCompanyInvest)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/companyInvest")
  }

  private def writeWorkIn(self: RDD[WorkIn]): Unit = {
    val rawWorkIn = self.map { workIn =>
      WorkInRaw(workIn.getPersonId, workIn.getCompanyId)
    }
    val df = spark.createDataFrame(rawWorkIn)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/workIn")
  }

  private def writeSignIn(self: RDD[SignIn]): Unit = {
    val rawSignIn = self.map { signIn =>
      SignInRaw(signIn.getMediumId, signIn.getAccountId, signIn.getCreationDate)
    }
    val df = spark.createDataFrame(rawSignIn)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/signIn")
  }

  private def writePersonGua(self: RDD[PersonGuaranteePerson]): Unit = {
    val rawPersonGua = self.map { guarantee =>
      PersonGuaranteePersonRaw(guarantee.getFromPersonId,
        guarantee.getToPersonId,
        guarantee.getCreationDate)
    }
    val df = spark.createDataFrame(rawPersonGua)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/personGuarantee")
  }

  private def writeCompanyGua(self: RDD[CompanyGuaranteeCompany]): Unit = {
    val rawCompanyGua = self.map { guarantee =>
      CompanyGuaranteeCompanyRaw(guarantee.getFromCompanyId,
        guarantee.getToCompanyId,
        guarantee.getCreationDate)
    }
    val df = spark.createDataFrame(rawCompanyGua)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/companyGuarantee")
  }

  private def writePersonLoan(self: RDD[PersonApplyLoan]): Unit = {
    val rawPersonApplyLoan = self.map { applyLoan =>
      PersonApplyLoanRaw(applyLoan.getPersonId, applyLoan.getLoanId, applyLoan.getCreationDate)
    }
    val df = spark.createDataFrame(rawPersonApplyLoan)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/personApplyLoan")
  }

  private def writeCompanyLoan(self: RDD[CompanyApplyLoan]): Unit = {
    val rawCompanyLoan = self.map { applyLoan =>
      CompanyApplyLoanRaw(applyLoan.getCompanyId, applyLoan.getLoanId, applyLoan.getCreationDate)
    }
    val df = spark.createDataFrame(rawCompanyLoan)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/companyApplyLoan")
  }

  private def writeLoan(self: RDD[Loan]): Unit = {
    val rawLoan = self.map { loan =>
      LoanRaw(loan.getLoanId, loan.getLoanAmount, loan.getBalance)
    }
    val df = spark.createDataFrame(rawLoan)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/loan")
  }

  private def writeTransfer(self: RDD[Transfer]): Unit = {
    val rawTransfer = self.map { transfer =>
      TransferRaw(transfer.getFromAccountId,
        transfer.getToAccountId,
        transfer.getCreationDate,
        random.nextLong(),
        "type")
    }
    val df = spark.createDataFrame(rawTransfer)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/transfer")
  }

  private def writeWithdraw(self: RDD[Withdraw]): Unit = {
    val rawWithdraw = self.map { withdraw =>
      WithdrawRaw(withdraw.getFromAccountId,
        withdraw.getToAccountId,
        withdraw.getCreationDate,
        random.nextLong())
    }
    val df = spark.createDataFrame(rawWithdraw)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/withdraw")
  }

  private def writeDeposit(self: RDD[Deposit]): Unit = {
    val rawDeposit = self.map { deposit =>
      DepositRaw(deposit.getLoanId,
        deposit.getAccountId,
        deposit.getCreationDate,
        deposit.getLoanAmount)
    }
    val df = spark.createDataFrame(rawDeposit)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/deposit")
  }

  private def writeRepay(self: RDD[Repay]): Unit = {
    val rawRepay = self.map { repay =>
      RepayRaw(repay.getAccountId, repay.getLoanId, repay.getCreationDate, random.nextLong())
    }
    val df = spark.createDataFrame(rawRepay)
    df.write
      .format(sink.format.toString)
      .options(options)
      .save(sink.outputDir + "/repay")

  }

  private def writeEvent(personRdd: RDD[Person],
                         companyRdd: RDD[Company],
                         mediumRdd: RDD[Medium]): Unit = {
    val activityGenerator = new ActivityGenerator(conf)

    val personOwnAccountInfo = activityGenerator.personRegisterEvent(personRdd)
    // extract personOwnAccount relationship
    val personOwnAccount: RDD[PersonOwnAccountRaw] =
      personOwnAccountInfo.map(personOwnAccountRaw => {
        PersonOwnAccountRaw(personOwnAccountRaw.getPersonId, personOwnAccountRaw.getAccountId)
      })

    // extract accountInfo
    val accountInfo1 = personOwnAccountInfo.map(personOwnAccountRaw => {
      new Account(
        personOwnAccountRaw.getAccountId,
        personOwnAccountRaw.getAccountType,
        personOwnAccountRaw.getAccountCreationDate,
        10000,
        personOwnAccountRaw.isAccountIsBlocked
      )
    })

    val companyOwnAccountInfo = activityGenerator.companyRegisterEvent(companyRdd)
    // extract companyOwnAccount relationship
    val companyOwnAccount: RDD[CompanyOwnAccountRaw] =
      companyOwnAccountInfo.map(companyOwnAccountRaw => {
        CompanyOwnAccountRaw(companyOwnAccountRaw.getCompanyId, companyOwnAccountRaw.getAccountId)
      })

    // extract accountInfo
    val accountInfo2 = companyOwnAccountInfo.map(companyOwnAccountRaw => {
      new Account(
        companyOwnAccountRaw.getAccountId,
        companyOwnAccountRaw.getAccountType,
        companyOwnAccountRaw.getAccountCreationDate,
        10000,
        companyOwnAccountRaw.isAccountIsBlocked
      )
    })

    val accountRdd = accountInfo1.union(accountInfo2)

    val personInvestRdd = activityGenerator.personInvestEvent(personRdd, companyRdd)
    val companyInvestRdd = activityGenerator.companyInvestEvent(companyRdd)
    val workInRdd = activityGenerator.workInEvent(personRdd, companyRdd)
    val signRdd = activityGenerator.signEvent(mediumRdd, accountRdd)

    val personGuaranteeRdd = activityGenerator.personGuaranteeEvent(personRdd)
    val companyGuaranteeRdd = activityGenerator.companyGuaranteeEvent(companyRdd)
    val personLoanRdd = activityGenerator.personLoanEvent(personRdd)
    val companyLoadRdd = activityGenerator.companyLoanEvent(companyRdd)
    val loanInfo1 = personLoanRdd.map(personApplyLoan => {
      new Loan(personApplyLoan.getLoanId,
        personApplyLoan.getLoanAmount,
        personApplyLoan.getLoanBalance,
        personApplyLoan.getCreationDate,
        10)
    })
    val loanInfo2 = companyLoadRdd.map(companyApplyLoan => {
      new Loan(companyApplyLoan.getLoanId,
        companyApplyLoan.getLoanAmount,
        companyApplyLoan.getLoanBalance,
        companyApplyLoan.getCreationDate,
        10)
    })
    val loanRdd = loanInfo1.union(loanInfo2)

    val transferRdd = activityGenerator.transferEvent(accountRdd)
    val withdrawRdd = activityGenerator.withdrawEvent(accountRdd)
    val depositRdd = activityGenerator.depositEvent(loanRdd, accountRdd)
    val repayRdd = activityGenerator.repayEvent(accountRdd, loanRdd)

    writePersonOwnAccount(personOwnAccount)
    writeCompanyOwnAccount(companyOwnAccount)
    writeAccount(accountRdd)
    writePersonInvest(personInvestRdd)
    writeCompanyInvest(companyInvestRdd)
    writeWorkIn(workInRdd)
    writeSignIn(signRdd)
    writePersonGua(personGuaranteeRdd)
    writeCompanyGua(companyGuaranteeRdd)
    writePersonLoan(personLoanRdd)
    writeCompanyLoan(companyLoadRdd)
    writeLoan(loanRdd)
    writeTransfer(transferRdd)
    writeWithdraw(withdrawRdd)
    writeDeposit(depositRdd)
    writeRepay(repayRdd)
  }
}
