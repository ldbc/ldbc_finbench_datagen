package ldbc.finbench.datagen.generation.serializers

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.model.raw._
import ldbc.finbench.datagen.syntax._
import ldbc.finbench.datagen.util.{Logging, SparkUI}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import scala.collection.JavaConverters._

/**
 * generate person and company activities
 * */
class ActivitySerializer(sink: RawSink, options: Map[String, String])(implicit spark: SparkSession) extends Serializable with Logging {
  private val pathPrefix:String = (sink.outputDir / "raw").toString

  def writePersonWithActivities(self: RDD[Person]): Unit = {
    SparkUI.jobAsync("Write Person", "Write Person") {
      val rawPersons = self.map { p: Person => PersonRaw(p.getPersonId, p.getCreationDate, p.getPersonName, p.isBlocked) }
      spark.createDataFrame(rawPersons).write.format(sink.format.toString).options(options).save((pathPrefix / "person").toString)
    }

    SparkUI.jobAsync("Write Person", "Write Person own account") {
      val rawPersonOwnAccount = self.flatMap { p =>
        p.getPersonOwnAccounts.asScala.map { poa =>
          PersonOwnAccountRaw(p.getPersonId, poa.getAccount.getAccountId, poa.getCreationDate, poa.getDeletionDate, poa.isExplicitlyDeleted)
        }
      }
      spark.createDataFrame(rawPersonOwnAccount).write.format(sink.format.toString).options(options).save((pathPrefix / "personOwnAccount").toString)
    }

    SparkUI.jobAsync("Write Person", "Write Person guarantee") {
      val rawPersonGuarantee = self.flatMap { p =>
        p.getGuaranteeSrc.asScala.map {
          pgp: PersonGuaranteePerson => PersonGuaranteePersonRaw(pgp.getFromPerson.getPersonId, pgp.getToPerson.getPersonId, pgp.getCreationDate)
        }
      }
      spark.createDataFrame(rawPersonGuarantee).write.format(sink.format.toString).options(options).save((pathPrefix / "personGuarantee").toString)
    }

    SparkUI.jobAsync("Write Person", "Write Person apply loan") {
      val rawPersonLoan = self.flatMap { p =>
        p.getPersonApplyLoans.asScala.map {
          pal: PersonApplyLoan => PersonApplyLoanRaw(pal.getPerson.getPersonId, pal.getLoan.getLoanId, pal.getLoan.getLoanAmount, pal.getCreationDate)
        }
      }
      spark.createDataFrame(rawPersonLoan).write.format(sink.format.toString).options(options).save((pathPrefix / "personApplyLoan").toString)
    }
  }

  def writeCompanyWithActivities(self: RDD[Company]): Unit = {
    SparkUI.jobAsync("Write Company", "Write Company") {
      val rawCompanies = self.map { c: Company => CompanyRaw(c.getCompanyId, c.getCreationDate, c.getCompanyName, c.isBlocked) }
      spark.createDataFrame(rawCompanies).write.format(sink.format.toString).options(options).save((pathPrefix / "company").toString)

      val rawCompanyOwnAccount = self.flatMap { c =>
        c.getCompanyOwnAccounts.asScala.map { coa =>
          CompanyOwnAccountRaw(c.getCompanyId, coa.getAccount.getAccountId, coa.getCreationDate, coa.getDeletionDate, coa.isExplicitlyDeleted)
        }
      }
      spark.createDataFrame(rawCompanyOwnAccount).write.format(sink.format.toString).options(options).save((pathPrefix / "companyOwnAccount").toString)
    }

    SparkUI.jobAsync("Write Company", "Write Company guarantee") {
      val rawCompanyGuarantee = self.flatMap { c =>
        c.getGuaranteeSrc.asScala.map {
          cgc: CompanyGuaranteeCompany => CompanyGuaranteeCompanyRaw(cgc.getFromCompany.getCompanyId, cgc.getToCompany.getCompanyId, cgc.getCreationDate)
        }
      }
      spark.createDataFrame(rawCompanyGuarantee).write.format(sink.format.toString).options(options).save((pathPrefix / "companyGuarantee").toString)
    }

    SparkUI.jobAsync("Write Company", "Write Company apply loan") {
      val rawCompanyLoan = self.flatMap { c =>
        c.getCompanyApplyLoans.asScala.map {
          cal: CompanyApplyLoan => CompanyApplyLoanRaw(cal.getCompany.getCompanyId, cal.getLoan.getLoanId, cal.getLoan.getLoanAmount, cal.getCreationDate)
        }
      }
      spark.createDataFrame(rawCompanyLoan).write.format(sink.format.toString).options(options).save((pathPrefix / "companyApplyLoan").toString)
    }
  }

  def writeMediumWithActivities(media: RDD[Medium], signIns: RDD[SignIn]): Unit = {
    SparkUI.jobAsync("Write media", "Write Medium") {
      val rawMedium = media.map { m: Medium => MediumRaw(m.getMediumId, m.getCreationDate, m.getMediumName, m.isBlocked) }
      spark.createDataFrame(rawMedium).write.format(sink.format.toString).options(options).save((pathPrefix / "medium").toString)

      val rawSignIn = signIns.map { si: SignIn => SignInRaw(si.getMedium.getMediumId, si.getAccount.getAccountId, si.getMultiplicityId, si.getCreationDate, si.getDeletionDate, si.isExplicitlyDeleted) }
      spark.createDataFrame(rawSignIn).write.format(sink.format.toString).options(options).save((pathPrefix / "signIn").toString)
    }
  }

  def writeAccountWithActivities(self: RDD[Account], transfers: RDD[Transfer]): Unit = {
    SparkUI.jobAsync("Write Account", "Write Account") {
      val rawAccount = self.map { a: Account => AccountRaw(a.getAccountId, a.getCreationDate, a.getDeletionDate, a.isBlocked, a.getType, a.getMaxInDegree, a.getMaxOutDegree, a.isExplicitlyDeleted, a.getOwnerType.toString) }
      spark.createDataFrame(rawAccount).write.format(sink.format.toString).options(options).save((pathPrefix / "account").toString)

      val rawTransfer = transfers.map { t =>
        TransferRaw(t.getFromAccount.getAccountId, t.getToAccount.getAccountId, t.getMultiplicityId, t.getCreationDate, t.getDeletionDate, t.getAmount, t.isExplicitlyDeleted)
      }
      spark.createDataFrame(rawTransfer).write.format(sink.format.toString).options(options).save((pathPrefix / "transfer").toString)
    }
  }

  def writeWithdraw(self: RDD[Withdraw]): Unit = {
    SparkUI.jobAsync("Write withdraw", "Write Withdraw") {
      val rawWithdraw = self.map {
        w => WithdrawRaw(w.getFromAccount.getAccountId, w.getToAccount.getAccountId, w.getFromAccount.getType, w.getToAccount.getType, w.getMultiplicityId, w.getCreationDate, w.getDeletionDate, w.getAmount, w.isExplicitlyDeleted)
      }
      spark.createDataFrame(rawWithdraw).write.format(sink.format.toString).options(options).save((pathPrefix / "withdraw").toString)
    }
  }

  def writeInvest(self: RDD[Either[PersonInvestCompany, CompanyInvestCompany]]): Unit = {
    SparkUI.jobAsync("Write invest", "Write Person Invest") {
      val personInvest = self.filter(_.isLeft).map(_.left.get)
      spark.createDataFrame(personInvest.map { pic =>
        PersonInvestCompanyRaw(pic.getPerson.getPersonId, pic.getCompany.getCompanyId, pic.getCreationDate, pic.getRatio)
      }).write.format(sink.format.toString).options(options).save((pathPrefix / "personInvest").toString)
    }

    SparkUI.jobAsync("Write invest", "Write Company Invest") {
      val companyInvest = self.filter(_.isRight).map(_.right.get)
      spark.createDataFrame(companyInvest.map { cic =>
        CompanyInvestCompanyRaw(cic.getFromCompany.getCompanyId, cic.getToCompany.getCompanyId, cic.getCreationDate, cic.getRatio)
      }).write.format(sink.format.toString).options(options).save((pathPrefix / "companyInvest").toString)
    }
  }

  def writeLoanActivities(self: RDD[Loan], deposits: RDD[Deposit], repays: RDD[Repay], loantransfers: RDD[Transfer]): Unit = {
    SparkUI.jobAsync("Write loan", "Write Loan") {
      val rawLoan = self.map { l: Loan => LoanRaw(l.getLoanId, l.getLoanAmount, l.getBalance) }
      spark.createDataFrame(rawLoan).write.format(sink.format.toString).options(options).save((pathPrefix / "loan").toString)

      val rawDeposit = deposits.map { d: Deposit => DepositRaw(d.getLoan.getLoanId, d.getAccount.getAccountId, d.getCreationDate, d.getDeletionDate, d.getAmount, d.isExplicitlyDeleted) }
      spark.createDataFrame(rawDeposit).write.format(sink.format.toString).options(options).save((pathPrefix / "deposit").toString)

      val rawRepay = repays.map { r: Repay => RepayRaw(r.getAccount.getAccountId, r.getLoan.getLoanId, r.getCreationDate, r.getDeletionDate, r.getAmount, r.isExplicitlyDeleted) }
      spark.createDataFrame(rawRepay).write.format(sink.format.toString).options(options).save((pathPrefix / "repay").toString)

      val rawLoanTransfer = loantransfers.map { t: Transfer => TransferRaw(t.getFromAccount.getAccountId, t.getToAccount.getAccountId, t.getMultiplicityId, t.getCreationDate, t.getDeletionDate, t.getAmount, t.isExplicitlyDeleted) }
      spark.createDataFrame(rawLoanTransfer).write.format(sink.format.toString).options(options).save((pathPrefix / "loantransfer").toString)
    }
  }
}
