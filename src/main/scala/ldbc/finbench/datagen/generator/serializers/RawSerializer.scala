package ldbc.finbench.datagen.generator.serializers

import ldbc.finbench.datagen.entities.edges.{
  CompanyApplyLoan,
  CompanyGuaranteeCompany,
  CompanyInvestCompany,
  PersonApplyLoan,
  PersonGuaranteePerson,
  PersonInvestCompany,
  SignIn,
  Transfer,
  Withdraw,
  WorkIn
}
import ldbc.finbench.datagen.entities.nodes.{Account, Company, Medium, Person}
import ldbc.finbench.datagen.generator.generators.{ActivityGenerator, SparkRanker}
import ldbc.finbench.datagen.io.Writer
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.model.raw.{
  AccountRaw,
  CompanyApplyLoanRaw,
  CompanyGuaranteeCompanyRaw,
  CompanyInvestCompanyRaw,
  CompanyOwnAccountRaw,
  CompanyRaw,
  MediumRaw,
  PersonApplyLoanRaw,
  PersonGuaranteePersonRaw,
  PersonInvestCompanyRaw,
  PersonOwnAccountRaw,
  PersonRaw,
  SignInRaw,
  TransferRaw,
  WithdrawRaw,
  WorkInRaw
}
import ldbc.finbench.datagen.util.GeneratorConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class RawSerializer(sink: RawSink, conf: GeneratorConfiguration)(implicit spark: SparkSession)
    extends Writer[RawSink] {
  def write(personRdd: RDD[Person], companyRdd: RDD[Company], mediumRdd: RDD[Medium]): Unit = {
    writePerson(personRdd)
    writeCompany(companyRdd)
    writeMedium(mediumRdd)
    writeEvent(personRdd, companyRdd, mediumRdd)
  }

  private def writePerson(self: RDD[Person]): Unit = {
    val rawPersons = self.map { p: Person =>
      PersonRaw(p.getPersonId, p.getPersonName, p.isBlocked)
    }
    val df = spark.createDataFrame(rawPersons)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writeCompany(self: RDD[Company]): Unit = {
    val rawCompanies = self.map { c: Company =>
      CompanyRaw(c.getCompanyId, c.getCompanyName, c.isBlocked)
    }
    val df = spark.createDataFrame(rawCompanies)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writeMedium(self: RDD[Medium]): Unit = {
    val rawMedium = self.map { m: Medium =>
      MediumRaw(m.getMediumId, m.getMediumName, m.isBlocked)
    }
    val df = spark.createDataFrame(rawMedium)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writeAccount(self: RDD[Account]): Unit = {
    val rawAccount = self.map { a: Account =>
      AccountRaw(a.getAccountId, a.getCreationDate, a.isBlocked, a.getType)
    }
    val df = spark.createDataFrame(rawAccount)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writePersonOwnAccount(self: RDD[PersonOwnAccountRaw]): Unit = {
    val df = spark.createDataFrame(self)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writeCompanyOwnAccount(self: RDD[CompanyOwnAccountRaw]): Unit = {
    val df = spark.createDataFrame(self)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writePersonInvest(self: RDD[PersonInvestCompany]): Unit = {
    val rawPersonInvest = self.map { invest =>
      PersonInvestCompanyRaw(invest.getPersonId, invest.getCompanyId)
    }
    val df = spark.createDataFrame(rawPersonInvest)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writeCompanyInvest(self: RDD[CompanyInvestCompany]): Unit = {
    val rawCompanyInvest = self.map { invest =>
      CompanyInvestCompanyRaw(invest.getFromCompanyId, invest.getToCompanyId)
    }
    val df = spark.createDataFrame(rawCompanyInvest)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writeWorkIn(self: RDD[WorkIn]): Unit = {
    val rawWorkIn = self.map { workIn =>
      WorkInRaw(workIn.getPersonId, workIn.getCompanyId)
    }
    val df = spark.createDataFrame(rawWorkIn)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writeSignIn(self: RDD[SignIn]): Unit = {
    val rawSignIn = self.map { signIn =>
      SignInRaw(signIn.getMediumId, signIn.getAccountId)
    }
    val df = spark.createDataFrame(rawSignIn)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writePersonGua(self: RDD[PersonGuaranteePerson]): Unit = {
    val rawPersonGua = self.map { guarantee =>
      PersonGuaranteePersonRaw(guarantee.getFromPersonId, guarantee.getToPersonId)
    }
    val df = spark.createDataFrame(rawPersonGua)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writeCompanyGua(self: RDD[CompanyGuaranteeCompany]): Unit = {
    val rawCompanyGua = self.map { guarantee =>
      CompanyGuaranteeCompanyRaw(guarantee.getFromCompanyId, guarantee.getToCompanyId)
    }
    val df = spark.createDataFrame(rawCompanyGua)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writePersonLoan(self: RDD[PersonApplyLoan]): Unit = {
    val rawPersonApplyLoan = self.map { applyLoan =>
      PersonApplyLoanRaw(applyLoan.getPersonId, applyLoan.getLoanId)
    }
    val df = spark.createDataFrame(rawPersonApplyLoan)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writeCompanyLoad(self: RDD[CompanyApplyLoan]): Unit = {
    val rawCompanyLoan = self.map { applyLoan =>
      CompanyApplyLoanRaw(applyLoan.getCompanyId, applyLoan.getLoanId)
    }
    val df = spark.createDataFrame(rawCompanyLoan)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writeTransfer(self: RDD[Transfer]): Unit = {
    val rawTransfer = self.map { transfer =>
      TransferRaw(transfer.getFromAccountId, transfer.getToAccountId)
    }
    val df = spark.createDataFrame(rawTransfer)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
  }

  private def writeWithdraw(self: RDD[Withdraw]): Unit = {
    val rawWithdraw = self.map { withdraw =>
      WithdrawRaw(withdraw.getFromAccountId, withdraw.getToAccountId)
    }
    val df = spark.createDataFrame(rawWithdraw)
    df.write.format(sink.format.toString).options(sink.formatOptions).save(sink.outputDir)
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

    val personInvestRdd  = activityGenerator.personInvestEvent(personRdd, companyRdd)
    val companyInvestRdd = activityGenerator.companyInvestEvent(companyRdd)
    val workInRdd        = activityGenerator.workInEvent(personRdd, companyRdd)
    val signRdd          = activityGenerator.signEvent(mediumRdd, accountRdd)

    val personGuaranteeRdd  = activityGenerator.personGuaranteeEvent(personRdd)
    val companyGuaranteeRdd = activityGenerator.companyGuaranteeEvent(companyRdd)
    val personLoanRdd       = activityGenerator.personLoanEvent(personRdd)
    val companyLoadRdd      = activityGenerator.companyLoanEvent(companyRdd)
    val transferRdd         = activityGenerator.transferEvent(accountRdd)
    val withdrawRdd         = activityGenerator.withdrawEvent(accountRdd)

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
    writeCompanyLoad(companyLoadRdd)
    writeTransfer(transferRdd)
    writeWithdraw(withdrawRdd)
  }
}
