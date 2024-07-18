package ldbc.finbench.datagen.model

import org.joda.time.DateTime

/**
  * define LDBC Finbench Data Schema
  */
object raw {

  // define Person entity
  case class PersonRaw(
      id: Long,
      createTime: Long,
      name: String,
      isBlocked: Boolean,
      gender: String,
      birthday: Long,
      country: String,
      city: String
  ) {
    override def toString: String = {
      s"$id|$createTime|$name|$isBlocked|$gender|$birthday|$country|$city"
    }
  }

  // define Account entity
  case class AccountRaw(
      id: Long,
      createTime: Long,
      deleteTime: Long,
      isBlocked: Boolean,
      `type`: String,
      nickname: String,
      phonenum: String,
      email: String,
      freqLoginType: String,
      lastLoginTime: Long,
      accountLevel: String,
      inDegree: Long,
      OutDegree: Long,
      isExplicitDeleted: Boolean,
      Owner: String
  ) {
    override def toString: String = {
      s"$id|$createTime|$deleteTime|$isBlocked|${`type`}|$nickname|$phonenum|$email|$freqLoginType|$lastLoginTime|$accountLevel|$inDegree|$OutDegree|$isExplicitDeleted|$Owner"
    }
  }

  // define Company entity
  case class CompanyRaw(
      id: Long,
      createTime: Long,
      name: String,
      isBlocked: Boolean,
      country: String,
      city: String,
      business: String,
      description: String,
      url: String
  ) {
    override def toString: String = {
      s"$id|$createTime|$name|$isBlocked|$country|$city|$business|$description|$url"
    }
  }

  // define Loan entity
  case class LoanRaw(
      id: Long,
      createTime: Long,
      loanAmount: String,
      balance: String,
      usage: String,
      interestRate: String
  ) {
    override def toString: String = {
      s"$id|$createTime|$loanAmount|$balance|$usage|$interestRate"
    }
  }

  // define Medium entity
  case class MediumRaw(
      id: Long,
      createTime: Long,
      `type`: String,
      isBlocked: Boolean,
      lastLogin: Long,
      riskLevel: String,
  ) {
    override def toString: String = {
      s"$id|$createTime|${`type`}|$isBlocked|$lastLogin|$riskLevel"
    }
  }

  // define PersonApplyLoan relationship
  case class PersonApplyLoanRaw(
      personId: Long,
      loanId: Long,
      loanAmount: String,
      createTime: Long,
      org: String
  ) {
    override def toString: String = {
      s"$personId|$loanId|$loanAmount|$createTime|$org"
    }
  }

  // define CompanyApplyLoan relationship
  case class CompanyApplyLoanRaw(
      companyId: Long,
      loanId: Long,
      loanAmount: String,
      createTime: Long,
      org: String
  ) {
    override def toString: String = {
      s"$companyId|$loanId|$loanAmount|$createTime|$org"
    }
  }

  // define PersonInvestCompany relationship
  case class PersonInvestCompanyRaw(
      investorId: Long,
      companyId: Long,
      createTime: Long,
      ratio: Double
  ) {
    override def toString: String = {
      s"$investorId|$companyId|$createTime|$ratio"
    }
  }

  // define CompanyInvestCompany relationship
  case class CompanyInvestCompanyRaw(
      investorId: Long,
      companyId: Long,
      createTime: Long,
      ratio: Double
  ) {
    override def toString: String = {
      s"$investorId|$companyId|$createTime|$ratio"
    }
  }

  // define PersonGuaranteePerson relationship
  case class PersonGuaranteePersonRaw(
      fromId: Long,
      toId: Long,
      createTime: Long,
      relation: String
  ) {
    override def toString: String = {
      s"$fromId|$toId|$createTime|$relation"
    }
  }

  // define CompanyGuarantee relationship
  case class CompanyGuaranteeCompanyRaw(
      fromId: Long,
      toId: Long,
      createTime: Long,
      relation: String
  ) {
    override def toString: String = {
      s"$fromId|$toId|$createTime|$relation"
    }
  }

  //define PersonOwnAccount relationship
  case class PersonOwnAccountRaw(
      personId: Long,
      accountId: Long,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean
  ) {
    override def toString: String = {
      s"$personId|$accountId|$createTime|$deleteTime|$isExplicitDeleted"
    }
  }

  // define CompanyOwnAccount relationship
  case class CompanyOwnAccountRaw(
      companyId: Long,
      accountId: Long,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean
  ) {
    override def toString: String = {
      s"$companyId|$accountId|$createTime|$deleteTime|$isExplicitDeleted"
    }
  }

  // define Transfer relationship
  case class TransferRaw(
      fromId: Long,
      toId: Long,
      multiplicityId: Long,
      createTime: Long,
      deleteTime: Long,
      amount: String,
      isExplicitDeleted: Boolean,
      orderNum: String,
      comment: String,
      payType: String,
      goodsType: String
  ) {
    override def toString: String = {
      s"$fromId|$toId|$multiplicityId|$createTime|$deleteTime|$amount|$isExplicitDeleted|$orderNum|$comment|$payType|$goodsType"
    }
  }

  // define Withdraw relationship
  case class WithdrawRaw(
      fromId: Long,
      toId: Long,
      fromType: String,
      toType: String,
      multiplicityId: Long,
      createTime: Long,
      deleteTime: Long,
      amount: String,
      isExplicitDeleted: Boolean
  ) {
    override def toString: String = {
      s"$fromId|$toId|$fromType|$toType|$multiplicityId|$createTime|$deleteTime|$amount|$isExplicitDeleted"
    }
  }

  // define Repay relationship
  case class RepayRaw(
      accountId: Long,
      loanId: Long,
      createTime: Long,
      deleteTime: Long,
      amount: String,
      isExplicitDeleted: Boolean
  ) {
    override def toString: String = {
      s"$accountId|$loanId|$createTime|$deleteTime|$amount|$isExplicitDeleted"
    }
  }

  // define Deposit relationship
  case class DepositRaw(
      loanId: Long,
      accountId: Long,
      createTime: Long,
      deleteTime: Long,
      amount: String,
      isExplicitDeleted: Boolean
  ) {
    override def toString: String = {
      s"$loanId|$accountId|$createTime|$deleteTime|$amount|$isExplicitDeleted"
    }
  }

  // define SignIn relationship
  case class SignInRaw(
      mediumId: Long,
      accountId: Long,
      multiplicityId: Long,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean,
      location: String
  ) {
    override def toString: String = {
      s"$mediumId|$accountId|$multiplicityId|$createTime|$deleteTime|$isExplicitDeleted|$location"
    }
  }
}
