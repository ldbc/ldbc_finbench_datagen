package ldbc.finbench.datagen.generator.serializers

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.model.raw._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * generate person and company activities
 * TODO: use some syntax to implement serializer less verbose
 * */
class ActivitySerializer(sink: RawSink, options: Map[String, String])(implicit spark: SparkSession) extends Serializable {
  val random = new Random()

  def writePerson(self: RDD[Person]): Unit = {
    val rawPersons = self.map { p: Person => PersonRaw(p.getPersonId, p.getCreationDate, p.getPersonName, p.isBlocked) }
    val df = spark.createDataFrame(rawPersons)
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/person")
  }

  def writeCompany(self: RDD[Company]): Unit = {
    val rawCompanies = self.map { c: Company => CompanyRaw(c.getCompanyId, c.getCreationDate, c.getCompanyName, c.isBlocked) }
    val df = spark.createDataFrame(rawCompanies)
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/company")
  }

  def writeMedium(self: RDD[Medium]): Unit = {
    val rawMedium = self.map { m: Medium => MediumRaw(m.getMediumId, m.getCreationDate, m.getMediumName, m.isBlocked) }
    val df = spark.createDataFrame(rawMedium)
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/medium")
  }

  def writeAccount(self: RDD[Account]): Unit = {
    val rawAccount = self.map { a: Account => AccountRaw(a.getAccountId, a.getCreationDate, a.getDeletionDate, a.isBlocked, a.getType) }
    val df = spark.createDataFrame(rawAccount)
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/account")
  }

  def writePersonOwnAccount(self: RDD[PersonOwnAccount]): Unit = {
    val df = spark.createDataFrame(self.map(personOwnAccountRaw => {
      PersonOwnAccountRaw(personOwnAccountRaw.getPerson.getPersonId, personOwnAccountRaw.getAccount.getAccountId, personOwnAccountRaw.getCreationDate, personOwnAccountRaw.getDeletionDate)
    }))
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/personOwnAccount")
  }

  def writeCompanyOwnAccount(self: RDD[CompanyOwnAccount]): Unit = {
    val df = spark.createDataFrame(self.map(companyOwnAccountRaw => {
      CompanyOwnAccountRaw(companyOwnAccountRaw.getCompany.getCompanyId, companyOwnAccountRaw.getAccount.getAccountId, companyOwnAccountRaw.getCreationDate, companyOwnAccountRaw.getDeletionDate)
    }))
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/companyOwnAccount")
  }

  def writePersonInvest(self: RDD[PersonInvestCompany]): Unit = {
    val df = spark.createDataFrame(self.map { invest =>
      PersonInvestCompanyRaw(invest.getPerson.getPersonId, invest.getCompany.getCompanyId, invest.getCreationDate, random.nextFloat()) // TODO: design invest ratio
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/personInvest")
  }

  def writeCompanyInvest(self: RDD[CompanyInvestCompany]): Unit = {
    val df = spark.createDataFrame(self.map { invest =>
      CompanyInvestCompanyRaw(invest.getFromCompany.getCompanyId, invest.getToCompany.getCompanyId, invest.getCreationDate, random.nextFloat())
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/companyInvest")
  }

  def writeWorkIn(self: RDD[WorkIn]): Unit = {
    val df = spark.createDataFrame(self.map { workIn =>
      WorkInRaw(workIn.getPerson.getPersonId, workIn.getCompany.getCompanyId, workIn.getCreationDate)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/workIn")
  }

  def writeSignIn(self: RDD[SignIn]): Unit = {
    val df = spark.createDataFrame(self.map { signIn =>
      SignInRaw(signIn.getMedium.getMediumId, signIn.getAccount.getAccountId, signIn.getCreationDate, signIn.getDeletionDate)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/signIn")
  }

  def writePersonGuarantee(self: RDD[PersonGuaranteePerson]): Unit = {
    val df = spark.createDataFrame(self.map { guarantee =>
      PersonGuaranteePersonRaw(guarantee.getFromPerson.getPersonId, guarantee.getToPerson.getPersonId, guarantee.getCreationDate)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/personGuarantee")
  }

  def writeCompanyGuarantee(self: RDD[CompanyGuaranteeCompany]): Unit = {
    val df = spark.createDataFrame(self.map { guarantee =>
      CompanyGuaranteeCompanyRaw(guarantee.getFromCompany.getCompanyId, guarantee.getToCompany.getCompanyId, guarantee.getCreationDate)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/companyGuarantee")
  }

  def writePersonLoan(self: RDD[PersonApplyLoan]): Unit = {
    val df = spark.createDataFrame(self.map { applyLoan =>
      PersonApplyLoanRaw(applyLoan.getPerson.getPersonId, applyLoan.getLoan.getLoanId, applyLoan.getCreationDate)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/personApplyLoan")
  }

  def writeCompanyLoan(self: RDD[CompanyApplyLoan]): Unit = {
    val df = spark.createDataFrame(self.map { applyLoan =>
      CompanyApplyLoanRaw(applyLoan.getCompany.getCompanyId, applyLoan.getLoan.getLoanId, applyLoan.getCreationDate)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/companyApplyLoan")
  }

  def writeLoan(self: RDD[Loan]): Unit = {
    val df = spark.createDataFrame(self.map { loan =>
      LoanRaw(loan.getLoanId, loan.getLoanAmount, loan.getBalance)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/loan")
  }

  def writeTransfer(self: RDD[Transfer]): Unit = {
    val df = spark.createDataFrame(self.map { transfer =>
      TransferRaw(transfer.getFromAccount.getAccountId, transfer.getToAccount.getAccountId, transfer.getCreationDate, random.nextLong(), "type")
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/transfer")
  }

  def writeWithdraw(self: RDD[Withdraw]): Unit = {
    val df = spark.createDataFrame(self.map { withdraw =>
      WithdrawRaw(withdraw.getFromAccount.getAccountId, withdraw.getToAccount.getAccountId, withdraw.getCreationDate, random.nextLong())
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/withdraw")
  }

  def writeDeposit(self: RDD[Deposit]): Unit = {
    val df = spark.createDataFrame(self.map { deposit =>
      DepositRaw(deposit.getLoan.getLoanId, deposit.getAccount.getAccountId, deposit.getCreationDate, deposit.getLoan.getLoanAmount)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/deposit")
  }

  def writeRepay(self: RDD[Repay]): Unit = {
    val df = spark.createDataFrame(self.map { repay =>
      RepayRaw(repay.getAccount.getAccountId, repay.getLoan.getLoanId, repay.getCreationDate, random.nextLong())
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/repay")
  }
}
