package ldbc.finbench.datagen.generation.serializers

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.model.raw._
import ldbc.finbench.datagen.syntax._
import ldbc.finbench.datagen.util.{Logging, SparkUI}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * generate person and company activities
 * */
class ActivitySerializer(sink: RawSink, options: Map[String, String])
                        (implicit spark: SparkSession) extends Serializable with Logging {
  private val pathPrefix: String = (sink.outputDir / "raw").toString
  private val idFormat: String = "%019d"

  def writePersonWithActivities(self: RDD[Person]): Unit = {
    SparkUI.jobAsync("Write Person", "Write Person") {
      val rawPersons = self.map { p: Person =>
        PersonRaw(idFormat.format(p.getPersonId), p.getCreationDate, p.getPersonName, p.isBlocked,
                  p.getGender, p.getBirthday, p.getCountryName, p.getCityName)
      }
      spark.createDataFrame(rawPersons).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "person").toString)
    }

    SparkUI.jobAsync("Write Person", "Write Person own account") {
      val rawPersonOwnAccount = self.flatMap { p =>
        p.getPersonOwnAccounts.asScala.map { poa =>
          PersonOwnAccountRaw(idFormat.format(p.getPersonId), idFormat.format(poa.getAccount.getAccountId),
                              poa.getCreationDate, poa.getDeletionDate, poa.isExplicitlyDeleted)
        }
      }
      spark.createDataFrame(rawPersonOwnAccount).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "personOwnAccount").toString)
    }

    SparkUI.jobAsync("Write Person", "Write Person guarantee") {
      val rawPersonGuarantee = self.flatMap { p =>
        p.getGuaranteeSrc.asScala.map {
          pgp: PersonGuaranteePerson =>
            PersonGuaranteePersonRaw(idFormat.format(pgp.getFromPerson.getPersonId),
                                     idFormat.format(pgp.getToPerson.getPersonId),
                                     pgp.getCreationDate)
        }
      }
      spark.createDataFrame(rawPersonGuarantee).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "personGuarantee").toString)
    }

    SparkUI.jobAsync("Write Person", "Write Person apply loan") {
      val rawPersonLoan = self.flatMap { p =>
        p.getPersonApplyLoans.asScala.map {
          pal: PersonApplyLoan =>
            PersonApplyLoanRaw(idFormat.format(pal.getPerson.getPersonId),
                               idFormat.format(pal.getLoan.getLoanId), pal.getLoan.getLoanAmount,
                               pal.getCreationDate, pal.getOrganization)
        }
      }
      spark.createDataFrame(rawPersonLoan).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "personApplyLoan").toString)
    }
  }

  def writeCompanyWithActivities(self: RDD[Company]): Unit = {
    SparkUI.jobAsync("Write Company", "Write Company") {
      val rawCompanies = self.map { c: Company =>
        CompanyRaw(idFormat.format(c.getCompanyId), c.getCreationDate, c.getCompanyName, c.isBlocked,
                   c.getCountryName, c.getCityName, c.getBusiness, c.getDescription, c.getUrl)
      }
      spark.createDataFrame(rawCompanies).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "company").toString)

      val rawCompanyOwnAccount = self.flatMap { c =>
        c.getCompanyOwnAccounts.asScala.map { coa =>
          CompanyOwnAccountRaw(idFormat.format(c.getCompanyId), idFormat.format(coa.getAccount.getAccountId),
                               coa.getCreationDate, coa.getDeletionDate, coa.isExplicitlyDeleted)
        }
      }
      spark.createDataFrame(rawCompanyOwnAccount).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "companyOwnAccount").toString)
    }

    SparkUI.jobAsync("Write Company", "Write Company guarantee") {
      val rawCompanyGuarantee = self.flatMap { c =>
        c.getGuaranteeSrc.asScala.map {
          cgc: CompanyGuaranteeCompany =>
            CompanyGuaranteeCompanyRaw(idFormat.format(cgc.getFromCompany.getCompanyId),
                                       idFormat.format(cgc.getToCompany.getCompanyId),
                                       cgc.getCreationDate)
        }
      }
      spark.createDataFrame(rawCompanyGuarantee).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "companyGuarantee").toString)
    }

    SparkUI.jobAsync("Write Company", "Write Company apply loan") {
      val rawCompanyLoan = self.flatMap { c =>
        c.getCompanyApplyLoans.asScala.map {
          cal: CompanyApplyLoan =>
            CompanyApplyLoanRaw(idFormat.format(cal.getCompany.getCompanyId),
                                idFormat.format(cal.getLoan.getLoanId),
                                cal.getLoan.getLoanAmount, cal.getCreationDate, cal.getOrganization)
        }
      }
      spark.createDataFrame(rawCompanyLoan).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "companyApplyLoan").toString)
    }
  }

  def writeMediumWithActivities(media: RDD[Medium], signIns: RDD[SignIn]): Unit = {
    SparkUI.jobAsync("Write media", "Write Medium") {
      val rawMedium = media
        .map { m: Medium =>
          MediumRaw(idFormat.format(m.getMediumId), m.getCreationDate, m.getMediumName, m.isBlocked,
                    m.getLastLogin, m.getRiskLevel)
        }
      spark.createDataFrame(rawMedium).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "medium").toString)

      val rawSignIn = signIns.map { si: SignIn =>
        SignInRaw(idFormat.format(si.getMedium.getMediumId), idFormat.format(si.getAccount.getAccountId),
                  si.getMultiplicityId, si.getCreationDate, si.getDeletionDate, si.isExplicitlyDeleted, si.getLocation)
      }
      spark.createDataFrame(rawSignIn).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "signIn").toString)
    }
  }

  def writeAccountWithActivities(self: RDD[Account], transfers: RDD[Transfer]): Unit = {
    SparkUI.jobAsync("Write Account", "Write Account") {
      val rawAccount = self.map { a: Account =>
        AccountRaw(idFormat.format(a.getAccountId), a.getCreationDate, a.getDeletionDate, a.isBlocked,
                   a.getType, a.getNickname, a.getPhonenum, a.getEmail, a.getFreqLoginType,
                   a.getLastLoginTime, a.getAccountLevel, a.getMaxInDegree, a.getMaxOutDegree, a.isExplicitlyDeleted,
                   a.getOwnerType.toString)
      }
      spark.createDataFrame(rawAccount).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "account").toString)

      val rawTransfer = transfers.map { t =>
        TransferRaw(idFormat.format(t.getFromAccount.getAccountId), idFormat.format(t.getToAccount.getAccountId),
                    t.getMultiplicityId, t.getCreationDate, t.getDeletionDate, t.getAmount, t.isExplicitlyDeleted,
                    t.getOrdernum, t.getComment, t.getPayType, t.getGoodsType)
      }
      spark.createDataFrame(rawTransfer).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "transfer").toString)
    }
  }

  def writeWithdraw(self: RDD[Withdraw]): Unit = {
    SparkUI.jobAsync("Write withdraw", "Write Withdraw") {
      val rawWithdraw = self.map {
        w =>
          WithdrawRaw(idFormat.format(w.getFromAccount.getAccountId), idFormat.format(w.getToAccount.getAccountId),
                      w.getFromAccount.getType, w.getToAccount.getType, w.getMultiplicityId, w.getCreationDate,
                      w.getDeletionDate, w.getAmount, w.isExplicitlyDeleted)
      }
      spark.createDataFrame(rawWithdraw).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "withdraw").toString)
    }
  }

  def writeInvest(self: RDD[Either[PersonInvestCompany, CompanyInvestCompany]]): Unit = {
    SparkUI.jobAsync("Write invest", "Write Person Invest") {
      val personInvest = self.filter(_.isLeft).map(_.left.get)
      spark.createDataFrame(personInvest.map { pic =>
        PersonInvestCompanyRaw(idFormat.format(pic.getPerson.getPersonId), idFormat.format(pic.getCompany.getCompanyId),
                               pic.getCreationDate, pic.getRatio)
      }).write.format(sink.format.toString).options(options).save((pathPrefix / "personInvest").toString)
    }

    SparkUI.jobAsync("Write invest", "Write Company Invest") {
      val companyInvest = self.filter(_.isRight).map(_.right.get)
      spark.createDataFrame(companyInvest.map { cic =>
        CompanyInvestCompanyRaw(idFormat.format(cic.getFromCompany.getCompanyId),
                                idFormat.format(cic.getToCompany.getCompanyId), cic.getCreationDate, cic.getRatio)
      }).write.format(sink.format.toString).options(options).save((pathPrefix / "companyInvest").toString)
    }
  }

  def writeLoanActivities(self: RDD[Loan], deposits: RDD[Deposit], repays: RDD[Repay],
                          loantransfers: RDD[Transfer]): Unit = {
    SparkUI.jobAsync("Write loan", "Write Loan") {
      val rawLoan = self.map { l: Loan =>
        LoanRaw(idFormat.format(l.getLoanId), l.getCreationDate, l.getLoanAmount, l.getBalance, l.getUsage,
                l.getInterestRate)
      }
      spark.createDataFrame(rawLoan).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "loan").toString)

      val rawDeposit = deposits.map { d: Deposit =>
        DepositRaw(idFormat.format(d.getLoan.getLoanId), idFormat.format(d.getAccount.getAccountId),
                   d.getCreationDate, d.getDeletionDate, d.getAmount, d.isExplicitlyDeleted)
      }
      spark.createDataFrame(rawDeposit).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "deposit").toString)

      val rawRepay = repays.map { r: Repay =>
        RepayRaw(idFormat.format(r.getAccount.getAccountId), idFormat.format(r.getLoan.getLoanId),
                 r.getCreationDate, r.getDeletionDate, r.getAmount, r.isExplicitlyDeleted)
      }
      spark.createDataFrame(rawRepay).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "repay").toString)

      val rawLoanTransfer = loantransfers.map { t: Transfer =>
        TransferRaw(idFormat.format(t.getFromAccount.getAccountId),
                    idFormat.format(t.getToAccount.getAccountId), t.getMultiplicityId, t.getCreationDate,
                    t.getDeletionDate, t.getAmount, t.isExplicitlyDeleted, t.getOrdernum, t.getComment, t.getPayType,
                    t.getGoodsType)
      }
      spark.createDataFrame(rawLoanTransfer).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "loantransfer").toString)
    }
  }
}
