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
import scala.concurrent.Future

/** generate person and company activities
  */
class ActivitySerializer(sink: RawSink)(implicit spark: SparkSession)
    extends Serializable
    with Logging {
  private val options: Map[String, String] =
    sink.formatOptions ++ Map("header" -> "true", "delimiter" -> "|")
  private val pathPrefix: String = (sink.outputDir / "raw").toString

  private def formattedDouble(d: Double): String = f"$d%.2f"

  def writePersonWithActivities(
      self: RDD[Person]
  )(implicit spark: SparkSession): Seq[Future[Unit]] = {
    val futures = Seq(
      SparkUI.jobAsync("Write Person", "Write Person") {
        val rawPersons = self.map { p: Person =>
          PersonRaw(
            p.getPersonId,
            p.getCreationDate,
            p.getPersonName,
            p.isBlocked,
            p.getGender,
            p.getBirthday,
            p.getCountryName,
            p.getCityName
          )
        }
        spark
          .createDataFrame(rawPersons)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "person").toString)
      },
      SparkUI.jobAsync("Write Person own account", "Write Person own account") {
        val rawPersonOwnAccount = self.flatMap { p =>
          p.getPersonOwnAccounts.asScala.map { poa =>
            PersonOwnAccountRaw(
              p.getPersonId,
              poa.getAccount.getAccountId,
              poa.getCreationDate,
              poa.getDeletionDate,
              poa.isExplicitlyDeleted,
              poa.getComment
            )
          }
        }
        spark
          .createDataFrame(rawPersonOwnAccount)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "personOwnAccount").toString)
      },
      SparkUI.jobAsync("Write Person guarantee", "Write Person guarantee") {
        val rawPersonGuarantee = self.flatMap { p =>
          p.getGuaranteeSrc.asScala.map { pgp: PersonGuaranteePerson =>
            PersonGuaranteePersonRaw(
              pgp.getFromPerson.getPersonId,
              pgp.getToPerson.getPersonId,
              pgp.getCreationDate,
              pgp.getRelationship,
              pgp.getComment
            )
          }
        }
        spark
          .createDataFrame(rawPersonGuarantee)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "personGuarantee").toString)
      },
      SparkUI.jobAsync("Write Person apply loan", "Write Person apply loan") {
        val rawPersonLoan = self.flatMap { p =>
          p.getPersonApplyLoans.asScala.map { pal: PersonApplyLoan =>
            PersonApplyLoanRaw(
              pal.getPerson.getPersonId,
              pal.getLoan.getLoanId,
              formattedDouble(pal.getLoan.getLoanAmount),
              pal.getCreationDate,
              pal.getOrganization,
              pal.getComment
            )
          }
        }
        spark
          .createDataFrame(rawPersonLoan)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "personApplyLoan").toString)
      }
    )
    futures
  }

  def writeCompanyWithActivities(
      self: RDD[Company]
  )(implicit spark: SparkSession): Seq[Future[Unit]] = {

    val futures = Seq(
      SparkUI.jobAsync("Write Company", "Write Company") {
        val rawCompanies = self.map { c: Company =>
          CompanyRaw(
            c.getCompanyId,
            c.getCreationDate,
            c.getCompanyName,
            c.isBlocked,
            c.getCountryName,
            c.getCityName,
            c.getBusiness,
            c.getDescription,
            c.getUrl
          )
        }
        spark
          .createDataFrame(rawCompanies)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "company").toString)
      },
      SparkUI
        .jobAsync("Write Company own account", "Write Company own account") {
          val rawCompanyOwnAccount = self.flatMap { c =>
            c.getCompanyOwnAccounts.asScala.map { coa =>
              CompanyOwnAccountRaw(
                c.getCompanyId,
                coa.getAccount.getAccountId,
                coa.getCreationDate,
                coa.getDeletionDate,
                coa.isExplicitlyDeleted,
                coa.getComment
              )
            }
          }
          spark
            .createDataFrame(rawCompanyOwnAccount)
            .write
            .format(sink.format.toString)
            .options(options)
            .save((pathPrefix / "companyOwnAccount").toString)
        },
      SparkUI.jobAsync("Write Company guarantee", "Write Company guarantee") {
        val rawCompanyGuarantee = self.flatMap { c =>
          c.getGuaranteeSrc.asScala.map { cgc: CompanyGuaranteeCompany =>
            CompanyGuaranteeCompanyRaw(
              cgc.getFromCompany.getCompanyId,
              cgc.getToCompany.getCompanyId,
              cgc.getCreationDate,
              cgc.getRelationship,
              cgc.getComment
            )
          }
        }
        spark
          .createDataFrame(rawCompanyGuarantee)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "companyGuarantee").toString)
      },
      SparkUI.jobAsync("Write Company apply loan", "Write Company apply loan") {
        val rawCompanyLoan = self.flatMap { c =>
          c.getCompanyApplyLoans.asScala.map { cal: CompanyApplyLoan =>
            CompanyApplyLoanRaw(
              cal.getCompany.getCompanyId,
              cal.getLoan.getLoanId,
              formattedDouble(cal.getLoan.getLoanAmount),
              cal.getCreationDate,
              cal.getOrganization,
              cal.getComment
            )
          }
        }
        spark
          .createDataFrame(rawCompanyLoan)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "companyApplyLoan").toString)
      }
    )
    futures
  }

  def writeMediumWithActivities(media: RDD[Medium], signIns: RDD[SignIn])(
      implicit spark: SparkSession
  ): Seq[Future[Unit]] = {

    val futures = Seq(
      SparkUI.jobAsync("Write medum", "Write Medium") {
        val rawMedium = media.map { m: Medium =>
          MediumRaw(
            m.getMediumId,
            m.getCreationDate,
            m.getMediumName,
            m.isBlocked,
            m.getLastLogin,
            m.getRiskLevel
          )
        }
        spark
          .createDataFrame(rawMedium)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "medium").toString)
      },
      SparkUI.jobAsync("Write media signin", "Write Medium sign in") {
        val rawSignIn = signIns.map { si: SignIn =>
          SignInRaw(
            si.getMedium.getMediumId,
            si.getAccount.getAccountId,
            si.getMultiplicityId,
            si.getCreationDate,
            si.getDeletionDate,
            si.isExplicitlyDeleted,
            si.getLocation,
            si.getComment
          )
        }
        spark
          .createDataFrame(rawSignIn)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "signIn").toString)
      }
    )
    futures
  }

  def writeAccountWithActivities(self: RDD[Account], transfers: RDD[Transfer])(
      implicit spark: SparkSession
  ): Seq[Future[Unit]] = {
    val futures = Seq(
      SparkUI.jobAsync("Write Account", "Write Account") {
        val rawAccount = self.map { a: Account =>
          AccountRaw(
            a.getAccountId,
            a.getCreationDate,
            a.getDeletionDate,
            a.isBlocked,
            a.getType,
            a.getNickname,
            a.getPhonenum,
            a.getEmail,
            a.getFreqLoginType,
            a.getLastLoginTime,
            a.getAccountLevel,
            a.getMaxInDegree,
            a.getMaxOutDegree,
            a.isExplicitlyDeleted,
            a.getOwnerType.toString
          )
        }
        spark
          .createDataFrame(rawAccount)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "account").toString)
      },
      SparkUI.jobAsync("Write Account transfer", "Write Account transfer") {
        val rawTransfer = transfers.map { t =>
          TransferRaw(
            t.getFromAccount.getAccountId,
            t.getToAccount.getAccountId,
            t.getMultiplicityId,
            t.getCreationDate,
            t.getDeletionDate,
            formattedDouble(t.getAmount),
            t.isExplicitlyDeleted,
            t.getOrdernum,
            t.getComment,
            t.getPayType,
            t.getGoodsType
          )
        }
        spark
          .createDataFrame(rawTransfer)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "transfer").toString)
      }
    )
    futures
  }

  def writeWithdraw(
      self: RDD[Withdraw]
  )(implicit spark: SparkSession): Seq[Future[Unit]] = {
    val futures = Seq(
      SparkUI.jobAsync("Write withdraw", "Write Withdraw") {
        val rawWithdraw = self.map { w =>
          WithdrawRaw(
            w.getFromAccount.getAccountId,
            w.getToAccount.getAccountId,
            w.getFromAccount.getType,
            w.getToAccount.getType,
            w.getMultiplicityId,
            w.getCreationDate,
            w.getDeletionDate,
            formattedDouble(w.getAmount),
            w.isExplicitlyDeleted,
            w.getComment
          )
        }
        spark
          .createDataFrame(rawWithdraw)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "withdraw").toString)
      }
    )
    futures
  }

  def writeInvest(
      self: RDD[Company]
  )(implicit spark: SparkSession): Seq[Future[Unit]] = {
    val futures = Seq(
      SparkUI.jobAsync("Write person invest", "Write Person Invest") {
        spark
          .createDataFrame(self.flatMap { c =>
            c.getPersonInvestCompanies.asScala.map { pic =>
              PersonInvestCompanyRaw(
                pic.getPerson.getPersonId,
                pic.getCompany.getCompanyId,
                pic.getCreationDate,
                pic.getRatio,
                pic.getComment
              )
            }
          })
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "personInvest").toString)
      },
      SparkUI.jobAsync("Write company invest", "Write Company Invest") {
        spark
          .createDataFrame(self.flatMap { c =>
            c.getCompanyInvestCompanies.asScala.map { cic =>
              CompanyInvestCompanyRaw(
                cic.getFromCompany.getCompanyId,
                cic.getToCompany.getCompanyId,
                cic.getCreationDate,
                cic.getRatio,
                cic.getComment
              )
            }
          })
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "companyInvest").toString)
      }
    )
    futures
  }

  def writeLoanActivities(
      self: RDD[Loan],
      deposits: RDD[Deposit],
      repays: RDD[Repay],
      loantransfers: RDD[Transfer]
  )(implicit spark: SparkSession): Seq[Future[Unit]] = {

    val futures = Seq(
      SparkUI.jobAsync("Write loan", "Write Loan") {
        val rawLoan = self.map { l: Loan =>
          LoanRaw(
            l.getLoanId,
            l.getCreationDate,
            formattedDouble(l.getLoanAmount),
            formattedDouble(l.getBalance),
            l.getUsage,
            f"${l.getInterestRate}%.3f"
          )
        }
        spark
          .createDataFrame(rawLoan)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "loan").toString)
      },
      SparkUI.jobAsync("Write loan desposit", "Write Loan desposit") {
        val rawDeposit = deposits.map { d: Deposit =>
          DepositRaw(
            d.getLoan.getLoanId,
            d.getAccount.getAccountId,
            d.getCreationDate,
            d.getDeletionDate,
            formattedDouble(d.getAmount),
            d.isExplicitlyDeleted,
            d.getComment
          )
        }
        spark
          .createDataFrame(rawDeposit)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "deposit").toString)
      },
      SparkUI.jobAsync("Write loan repay", "Write Loan repay") {
        val rawRepay = repays.map { r: Repay =>
          RepayRaw(
            r.getAccount.getAccountId,
            r.getLoan.getLoanId,
            r.getCreationDate,
            r.getDeletionDate,
            formattedDouble(r.getAmount),
            r.isExplicitlyDeleted,
            r.getComment
          )
        }
        spark
          .createDataFrame(rawRepay)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "repay").toString)
      },
      SparkUI.jobAsync("Write loan transfer", "Write Loan transfer") {
        val rawLoanTransfer = loantransfers.map { t: Transfer =>
          TransferRaw(
            t.getFromAccount.getAccountId,
            t.getToAccount.getAccountId,
            t.getMultiplicityId,
            t.getCreationDate,
            t.getDeletionDate,
            formattedDouble(t.getAmount),
            t.isExplicitlyDeleted,
            t.getOrdernum,
            t.getComment,
            t.getPayType,
            t.getGoodsType
          )
        }
        spark
          .createDataFrame(rawLoanTransfer)
          .write
          .format(sink.format.toString)
          .options(options)
          .save((pathPrefix / "loantransfer").toString)
      }
    )
    futures
  }

}
