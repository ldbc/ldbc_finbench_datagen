package ldbc.finbench.datagen.generator.serializers

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.model.raw._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * generate person and company activities
 * TODO: use some syntax to implement serializer less verbose like GraphDef
 * */
class ActivitySerializer(sink: RawSink, options: Map[String, String])(implicit spark: SparkSession) extends Serializable {
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
    val rawAccount = self.map { a: Account => AccountRaw(a.getAccountId, a.getCreationDate, a.getDeletionDate, a.isBlocked, a.getType, a.getMaxInDegree, a.getMaxOutDegree, a.isExplicitlyDeleted) }
    val df = spark.createDataFrame(rawAccount)
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/account")
  }

  def writePersonOwnAccount(self: RDD[PersonOwnAccount]): Unit = {
    val df = spark.createDataFrame(self.map(poa => {
      PersonOwnAccountRaw(poa.getPerson.getPersonId, poa.getAccount.getAccountId, poa.getCreationDate, poa.getDeletionDate, poa.isExplicitlyDeleted)
    }))
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/personOwnAccount")
  }

  def writeCompanyOwnAccount(self: RDD[CompanyOwnAccount]): Unit = {
    val df = spark.createDataFrame(self.map(coa => {
      CompanyOwnAccountRaw(coa.getCompany.getCompanyId, coa.getAccount.getAccountId, coa.getCreationDate, coa.getDeletionDate, coa.isExplicitlyDeleted)
    }))
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/companyOwnAccount")
  }

  def writePersonInvest(self: RDD[PersonInvestCompany]): Unit = {
    val df = spark.createDataFrame(self.map { pic =>
      PersonInvestCompanyRaw(pic.getPerson.getPersonId, pic.getCompany.getCompanyId, pic.getCreationDate, pic.getRatio)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/personInvest")
  }

  def writeCompanyInvest(self: RDD[CompanyInvestCompany]): Unit = {
    val df = spark.createDataFrame(self.map { cic =>
      CompanyInvestCompanyRaw(cic.getFromCompany.getCompanyId, cic.getToCompany.getCompanyId, cic.getCreationDate, cic.getRatio)
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
      SignInRaw(signIn.getMedium.getMediumId, signIn.getAccount.getAccountId, signIn.getCreationDate, signIn.getDeletionDate, signIn.isExplicitlyDeleted)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/signIn")
  }

  def writePersonGuarantee(self: RDD[PersonGuaranteePerson]): Unit = {
    val df = spark.createDataFrame(self.map { pgp =>
      PersonGuaranteePersonRaw(pgp.getFromPerson.getPersonId, pgp.getToPerson.getPersonId, pgp.getCreationDate)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/personGuarantee")
  }

  def writeCompanyGuarantee(self: RDD[CompanyGuaranteeCompany]): Unit = {
    val df = spark.createDataFrame(self.map { cgc =>
      CompanyGuaranteeCompanyRaw(cgc.getFromCompany.getCompanyId, cgc.getToCompany.getCompanyId, cgc.getCreationDate)
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
    val df = spark.createDataFrame(self.map { t =>
      TransferRaw(t.getFromAccount.getAccountId, t.getToAccount.getAccountId, t.getCreationDate, t.getAmount, t.getType, t.isExplicitlyDeleted)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/transfer")
  }

  def writeWithdraw(self: RDD[Withdraw]): Unit = {
    val df = spark.createDataFrame(self.map { w =>
      WithdrawRaw(w.getFromAccount.getAccountId, w.getToAccount.getAccountId, w.getCreationDate, w.getAmount, w.isExplicitlyDeleted)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/withdraw")
  }

  def writeDeposit(self: RDD[Deposit]): Unit = {
    val df = spark.createDataFrame(self.map { d =>
      DepositRaw(d.getLoan.getLoanId, d.getAccount.getAccountId, d.getCreationDate, d.getAmount, d.isExplicitlyDeleted)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/deposit")
  }

  def writeRepay(self: RDD[Repay]): Unit = {
    val df = spark.createDataFrame(self.map { r =>
      RepayRaw(r.getAccount.getAccountId, r.getLoan.getLoanId, r.getCreationDate, r.getAmount, r.isExplicitlyDeleted)
    })
    df.write.format(sink.format.toString).options(options).save(sink.outputDir + "/repay")
  }
}
