package ldbc.finbench.datagen.generation.serializers

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.model.raw._
import ldbc.finbench.datagen.syntax._
import ldbc.finbench.datagen.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

/**
 * generate person and company activities
 * */
class ActivitySerializer(sink: RawSink, options: Map[String, String])(implicit spark: SparkSession) extends Serializable with Logging {
  private val pathPrefix = sink.outputDir / "raw"
  def writePerson(self: RDD[Person]): Unit = {
    val rawPersons = self.map { p: Person => PersonRaw(p.getPersonId, p.getCreationDate, p.getPersonName, p.isBlocked) }
    val df = spark.createDataFrame(rawPersons)
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "person").toString)
  }

  def writeCompany(self: RDD[Company]): Unit = {
    val rawCompanies = self.map { c: Company => CompanyRaw(c.getCompanyId, c.getCreationDate, c.getCompanyName, c.isBlocked) }
    val df = spark.createDataFrame(rawCompanies)
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "company").toString)
  }

  def writeMedium(self: RDD[Medium]): Unit = {
    val rawMedium = self.map { m: Medium => MediumRaw(m.getMediumId, m.getCreationDate, m.getMediumName, m.isBlocked) }
    val df = spark.createDataFrame(rawMedium)
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "medium").toString)
  }

  def writeAccount(self: RDD[Account]): Unit = {
    val rawAccount = self.map { a: Account => AccountRaw(a.getAccountId, a.getCreationDate, a.getDeletionDate, a.isBlocked, a.getType, a.getMaxInDegree, a.getMaxOutDegree, a.isExplicitlyDeleted, a.getOwnerType.toString) }
    val df = spark.createDataFrame(rawAccount)
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "account").toString)
  }

  def writePersonOwnAccount(self: RDD[PersonOwnAccount]): Unit = {
    val df = spark.createDataFrame(self.map(poa => {
      PersonOwnAccountRaw(poa.getPerson.getPersonId, poa.getAccount.getAccountId, poa.getCreationDate, poa.getDeletionDate, poa.isExplicitlyDeleted)
    }))
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "personOwnAccount").toString)
  }

  def writeCompanyOwnAccount(self: RDD[CompanyOwnAccount]): Unit = {
    val df = spark.createDataFrame(self.map(coa => {
      CompanyOwnAccountRaw(coa.getCompany.getCompanyId, coa.getAccount.getAccountId, coa.getCreationDate, coa.getDeletionDate, coa.isExplicitlyDeleted)
    }))
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "companyOwnAccount").toString)
  }

  def writeInvest(self: RDD[Either[PersonInvestCompany, CompanyInvestCompany]]): Unit = {
    val personInvest = self.filter(_.isLeft).map(_.left.get)
    spark.createDataFrame(personInvest.map { pic =>
      PersonInvestCompanyRaw(pic.getPerson.getPersonId, pic.getCompany.getCompanyId, pic.getCreationDate, pic.getRatio)
    }).write.format(sink.format.toString).options(options).save((pathPrefix / "personInvest").toString)

    val companyInvest = self.filter(_.isRight).map(_.right.get)
    spark.createDataFrame(companyInvest.map { cic =>
      CompanyInvestCompanyRaw(cic.getFromCompany.getCompanyId, cic.getToCompany.getCompanyId, cic.getCreationDate, cic.getRatio)
    }).write.format(sink.format.toString).options(options).save((pathPrefix / "companyInvest").toString)
  }

  def writeSignIn(self: RDD[SignIn]): Unit = {
    val df = spark.createDataFrame(self.map { signIn =>
      SignInRaw(signIn.getMedium.getMediumId, signIn.getAccount.getAccountId, signIn.getMultiplicityId, signIn.getCreationDate, signIn.getDeletionDate, signIn.isExplicitlyDeleted)
    })
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "signIn").toString)
  }

  def writePersonGuarantee(self: RDD[PersonGuaranteePerson]): Unit = {
    val df = spark.createDataFrame(self.map { pgp =>
      PersonGuaranteePersonRaw(pgp.getFromPerson.getPersonId, pgp.getToPerson.getPersonId, pgp.getCreationDate)
    })
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "personGuarantee").toString)
  }

  def writeCompanyGuarantee(self: RDD[CompanyGuaranteeCompany]): Unit = {
    val df = spark.createDataFrame(self.map { cgc =>
      CompanyGuaranteeCompanyRaw(cgc.getFromCompany.getCompanyId, cgc.getToCompany.getCompanyId, cgc.getCreationDate)
    })
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "companyGuarantee").toString)
  }

  def writePersonLoan(self: RDD[PersonApplyLoan]): Unit = {
    val df = spark.createDataFrame(self.map { apply =>
      PersonApplyLoanRaw(apply.getPerson.getPersonId, apply.getLoan.getLoanId, apply.getLoan.getLoanAmount, apply.getCreationDate)
    })
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "personApplyLoan").toString)
  }

  def writeCompanyLoan(self: RDD[CompanyApplyLoan]): Unit = {
    val df = spark.createDataFrame(self.map { apply =>
      CompanyApplyLoanRaw(apply.getCompany.getCompanyId, apply.getLoan.getLoanId, apply.getLoan.getLoanAmount, apply.getCreationDate)
    })
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "companyApplyLoan").toString)
  }

  def writeLoan(self: RDD[Loan]): Unit = {
    val df = spark.createDataFrame(self.map { loan =>
      LoanRaw(loan.getLoanId, loan.getLoanAmount, loan.getBalance)
    })
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "loan").toString)
  }


  def writeLoanTransfer(self: RDD[Transfer]): Unit = {
    val df = spark.createDataFrame(self.map { t =>
      TransferRaw(t.getFromAccount.getAccountId, t.getToAccount.getAccountId, t.getMultiplicityId, t.getCreationDate, t.getDeletionDate, t.getAmount, t.isExplicitlyDeleted)
    })
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "loantransfer").toString)
  }

  def writeTransfer(self: RDD[Transfer]): Unit = {
    val df = spark.createDataFrame(self.map { t =>
      TransferRaw(t.getFromAccount.getAccountId, t.getToAccount.getAccountId, t.getMultiplicityId, t.getCreationDate, t.getDeletionDate, t.getAmount, t.isExplicitlyDeleted)
    })
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "transfer").toString)
  }

  def writeWithdraw(self: RDD[Withdraw]): Unit = {
    val df = spark.createDataFrame(self.map { w =>
      WithdrawRaw(w.getFromAccount.getAccountId, w.getToAccount.getAccountId,
        w.getFromAccount.getType, w.getToAccount.getType, w.getMultiplicityId,
        w.getCreationDate, w.getDeletionDate, w.getAmount, w.isExplicitlyDeleted)
    })
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "withdraw").toString)
  }

  def writeDeposit(self: RDD[Deposit]): Unit = {
    val df = spark.createDataFrame(self.map { d =>
      DepositRaw(d.getLoan.getLoanId, d.getAccount.getAccountId, d.getCreationDate, d.getDeletionDate, d.getAmount, d.isExplicitlyDeleted)
    })
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "deposit").toString)
  }

  def writeRepay(self: RDD[Repay]): Unit = {
    val df = spark.createDataFrame(self.map { r =>
      RepayRaw(r.getAccount.getAccountId, r.getLoan.getLoanId, r.getCreationDate, r.getDeletionDate, r.getAmount, r.isExplicitlyDeleted)
    })
    df.write.format(sink.format.toString).options(options).save((pathPrefix / "repay").toString)
  }
}
