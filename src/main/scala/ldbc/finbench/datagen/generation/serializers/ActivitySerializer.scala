package ldbc.finbench.datagen.generation.serializers

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.model.raw._
import ldbc.finbench.datagen.util.{Logging, SparkUI}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * generate person and company activities
 * */
class ActivitySerializer(sink: RawSink, options: Map[String, String])(implicit spark: SparkSession) extends Serializable with Logging {
  def writePerson(self: RDD[Person]): Unit = {
    SparkUI.jobAsync("Write", "Write Person") {
      val rawPersons = self.map { p: Person => PersonRaw(p.getPersonId, p.getCreationDate, p.getPersonName, p.isBlocked) }
      spark.createDataFrame(rawPersons).write.format(sink.format.toString).options(options).save(sink.outputDir + "/person")
    }
  }

  def writeCompany(self: RDD[Company]): Unit = {
    SparkUI.jobAsync("Write", "Write Company") {
      val rawCompanies = self.map { c: Company => CompanyRaw(c.getCompanyId, c.getCreationDate, c.getCompanyName, c.isBlocked) }
      spark.createDataFrame(rawCompanies).write.format(sink.format.toString).options(options).save(sink.outputDir + "/company")
    }
  }

  def writeMedium(self: RDD[Medium]): Unit = {
    SparkUI.jobAsync("Write", "Write Medium") {
      val rawMedium = self.map { m: Medium => MediumRaw(m.getMediumId, m.getCreationDate, m.getMediumName, m.isBlocked) }
      spark.createDataFrame(rawMedium).write.format(sink.format.toString).options(options).save(sink.outputDir + "/medium")
    }
  }

  def writeAccount(self: RDD[Account]): Unit = {
    SparkUI.jobAsync("Write", "Write Account") {
      val rawAccount = self.map { a: Account => AccountRaw(a.getAccountId, a.getCreationDate, a.getDeletionDate, a.isBlocked, a.getType, a.getMaxInDegree, a.getMaxOutDegree, a.isExplicitlyDeleted, a.getOwnerType.toString) }
      spark.createDataFrame(rawAccount).write.format(sink.format.toString).options(options).save(sink.outputDir + "/account")
    }
  }

  def writePersonOwnAccount(self: RDD[PersonOwnAccount]): Unit = {
    SparkUI.jobAsync("Write", "Write PersonOwnAccount") {
      val rawPersonOwnAccount = self.map { poa: PersonOwnAccount => PersonOwnAccountRaw(poa.getPerson.getPersonId, poa.getAccount.getAccountId, poa.getCreationDate, poa.getDeletionDate, poa.isExplicitlyDeleted) }
      spark.createDataFrame(rawPersonOwnAccount).write.format(sink.format.toString).options(options).save(sink.outputDir + "/personOwnAccount")
    }
  }

  def writeCompanyOwnAccount(self: RDD[CompanyOwnAccount]): Unit = {
    SparkUI.jobAsync("Write", "Write CompanyOwnAccount") {
      val rawCompanyOwnAccount = self.map { coa: CompanyOwnAccount => CompanyOwnAccountRaw(coa.getCompany.getCompanyId, coa.getAccount.getAccountId, coa.getCreationDate, coa.getDeletionDate, coa.isExplicitlyDeleted) }
      spark.createDataFrame(rawCompanyOwnAccount).write.format(sink.format.toString).options(options).save(sink.outputDir + "/companyOwnAccount")
    }
  }

  def writeInvest(self: RDD[Either[PersonInvestCompany, CompanyInvestCompany]]): Unit = {
    SparkUI.jobAsync("Write", "Write Person Invest") {
      val personInvest = self.filter(_.isLeft).map(_.left.get)
      spark.createDataFrame(personInvest.map { pic =>
        PersonInvestCompanyRaw(pic.getPerson.getPersonId, pic.getCompany.getCompanyId, pic.getCreationDate, pic.getRatio)
      }).write.format(sink.format.toString).options(options).save(sink.outputDir + "/personInvest")
    }

    SparkUI.jobAsync("Write", "Write Company Invest") {
      val companyInvest = self.filter(_.isRight).map(_.right.get)
      spark.createDataFrame(companyInvest.map { cic =>
        CompanyInvestCompanyRaw(cic.getFromCompany.getCompanyId, cic.getToCompany.getCompanyId, cic.getCreationDate, cic.getRatio)
      }).write.format(sink.format.toString).options(options).save(sink.outputDir + "/companyInvest")
    }
  }

  def writeSignIn(self: RDD[SignIn]): Unit = {
    SparkUI.jobAsync("Write", "Write SignIn") {
      val rawSignIn = self.map { si: SignIn => SignInRaw(si.getMedium.getMediumId, si.getAccount.getAccountId, si.getMultiplicityId, si.getCreationDate, si.getDeletionDate, si.isExplicitlyDeleted) }
      spark.createDataFrame(rawSignIn).write.format(sink.format.toString).options(options).save(sink.outputDir + "/signIn")
    }
  }

  def writePersonGuarantee(self: RDD[PersonGuaranteePerson]): Unit = {
    SparkUI.jobAsync("Write", "Write PersonGuarantee") {
      val rawPersonGuarantee = self.map { pgp: PersonGuaranteePerson => PersonGuaranteePersonRaw(pgp.getFromPerson.getPersonId, pgp.getToPerson.getPersonId, pgp.getCreationDate) }
      spark.createDataFrame(rawPersonGuarantee).write.format(sink.format.toString).options(options).save(sink.outputDir + "/personGuarantee")
    }
  }

  def writeCompanyGuarantee(self: RDD[CompanyGuaranteeCompany]): Unit = {
    SparkUI.jobAsync("Write", "Write CompanyGuarantee") {
      val rawCompanyGuarantee = self.map { cgc: CompanyGuaranteeCompany => CompanyGuaranteeCompanyRaw(cgc.getFromCompany.getCompanyId, cgc.getToCompany.getCompanyId, cgc.getCreationDate) }
      spark.createDataFrame(rawCompanyGuarantee).write.format(sink.format.toString).options(options).save(sink.outputDir + "/companyGuarantee")
    }
  }

  def writePersonLoan(self: RDD[PersonApplyLoan]): Unit = {
    SparkUI.jobAsync("Write", "Write PersonLoan") {
      val rawPersonLoan = self.map { pal: PersonApplyLoan => PersonApplyLoanRaw(pal.getPerson.getPersonId, pal.getLoan.getLoanId, pal.getLoan.getLoanAmount, pal.getCreationDate) }
      spark.createDataFrame(rawPersonLoan).write.format(sink.format.toString).options(options).save(sink.outputDir + "/personLoan")
    }
  }

  def writeCompanyLoan(self: RDD[CompanyApplyLoan]): Unit = {
    SparkUI.jobAsync("Write", "Write CompanyLoan") {
      val rawCompanyLoan = self.map { cal: CompanyApplyLoan => CompanyApplyLoanRaw(cal.getCompany.getCompanyId, cal.getLoan.getLoanId, cal.getLoan.getLoanAmount, cal.getCreationDate) }
      spark.createDataFrame(rawCompanyLoan).write.format(sink.format.toString).options(options).save(sink.outputDir + "/companyLoan")
    }
  }

  def writeLoan(self: RDD[Loan]): Unit = {
    SparkUI.jobAsync("Write", "Write Loan") {
      val rawLoan = self.map { l: Loan => LoanRaw(l.getLoanId, l.getLoanAmount, l.getBalance) }
      spark.createDataFrame(rawLoan).write.format(sink.format.toString).options(options).save(sink.outputDir + "/loan")
    }
  }

  def writeLoanTransfer(self: RDD[Transfer]): Unit = {
    SparkUI.jobAsync("Write", "Write LoanTransfer") {
      val rawLoanTransfer = self.map { t: Transfer => TransferRaw(t.getFromAccount.getAccountId, t.getToAccount.getAccountId, t.getMultiplicityId, t.getCreationDate, t.getDeletionDate, t.getAmount, t.isExplicitlyDeleted) }
      spark.createDataFrame(rawLoanTransfer).write.format(sink.format.toString).options(options).save(sink.outputDir + "/loanTransfer")
    }
  }

  def writeTransfer(self: RDD[Transfer]): Unit = {
    SparkUI.jobAsync("Write", "Write Transfer") {
      val rawTransfer = self.map { t: Transfer => TransferRaw(t.getFromAccount.getAccountId, t.getToAccount.getAccountId, t.getMultiplicityId, t.getCreationDate, t.getDeletionDate, t.getAmount, t.isExplicitlyDeleted) }
      spark.createDataFrame(rawTransfer).write.format(sink.format.toString).options(options).save(sink.outputDir + "/transfer")
    }
  }

  def writeWithdraw(self: RDD[Withdraw]): Unit = {
    SparkUI.jobAsync("Write", "Write Withdraw") {
      val rawWithdraw = self.map { w: Withdraw => WithdrawRaw(w.getFromAccount.getAccountId, w.getToAccount.getAccountId, w.getFromAccount.getType, w.getToAccount.getType, w.getMultiplicityId, w.getCreationDate, w.getDeletionDate, w.getAmount, w.isExplicitlyDeleted) }
      spark.createDataFrame(rawWithdraw).write.format(sink.format.toString).options(options).save(sink.outputDir + "/withdraw")
    }
  }

  def writeDeposit(self: RDD[Deposit]): Unit = {
    SparkUI.jobAsync("Write", "Write Deposit") {
      val rawDeposit = self.map { d: Deposit => DepositRaw(d.getLoan.getLoanId, d.getAccount.getAccountId, d.getCreationDate, d.getDeletionDate, d.getAmount, d.isExplicitlyDeleted) }
      spark.createDataFrame(rawDeposit).write.format(sink.format.toString).options(options).save(sink.outputDir + "/deposit")
    }
  }

  def writeRepay(self: RDD[Repay]): Unit = {
    SparkUI.jobAsync("Write", "Write Repay") {
      val rawRepay = self.map { r: Repay => RepayRaw(r.getAccount.getAccountId, r.getLoan.getLoanId, r.getCreationDate, r.getDeletionDate, r.getAmount, r.isExplicitlyDeleted) }
      spark.createDataFrame(rawRepay).write.format(sink.format.toString).options(options).save(sink.outputDir + "/repay")
    }
  }
}
