package ldbc.finbench.datagen.model

import org.joda.time.DateTime

/**
  * define LDBC Finbench Data Schema
  */
object raw {

  // define Person entity
  case class PersonRaw(
      id: String,
      createTime: Long,
      name: String,
      isBlocked: Boolean,
      gender: String,
      birthday: Long,
      country: String,
      city: String
  )

  // define Account entity
  case class AccountRaw(
      id: String,
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
  )

  // define Company entity
  case class CompanyRaw(
      id: String,
      createTime: Long,
      name: String,
      isBlocked: Boolean,
      country: String,
      city: String,
      business: String,
      description: String,
      url: String
  )

  // define Loan entity
  case class LoanRaw(
      id: String,
      createTime: Long,
      loanAmount: Double,
      balance: Double,
      usage: String,
      interestRate: Double
  )

  // define Medium entity
  case class MediumRaw(
      id: String,
      createTime: Long,
      `type`: String,
      isBlocked: Boolean,
      lastLogin: Long,
      riskLevel: String,
  )

  // define PersonApplyLoan relationship
  case class PersonApplyLoanRaw(
      personId: String,
      loanId: String,
      loanAmount: Double,
      createTime: Long,
      org: String
  )

  // define CompanyApplyLoan relationship
  case class CompanyApplyLoanRaw(
      companyId: String,
      loanId: String,
      loanAmount: Double,
      createTime: Long,
      org: String
  )

  // define PersonInvestCompany relationship
  case class PersonInvestCompanyRaw(
      investorId: String,
      companyId: String,
      createTime: Long,
      ratio: Double
  )

  // define CompanyInvestCompany relationship
  case class CompanyInvestCompanyRaw(
      investorId: String,
      companyId: String,
      createTime: Long,
      ratio: Double
  )

  // define PersonGuaranteePerson relationship
  case class PersonGuaranteePersonRaw(
      fromId: String,
      toId: String,
      createTime: Long
  )

  // define CompanyGuarantee relationship
  case class CompanyGuaranteeCompanyRaw(
      fromId: String,
      toId: String,
      createTime: Long
  )

  //define PersonOwnAccount relationship
  case class PersonOwnAccountRaw(
      personId: String,
      accountId: String,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean
  )

  // define CompanyOwnAccount relationship
  case class CompanyOwnAccountRaw(
      companyId: String,
      accountId: String,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean
  )

  // define Transfer relationship
  case class TransferRaw(
      fromId: String,
      toId: String,
      multiplicityId: Long,
      createTime: Long,
      deleteTime: Long,
      amount: Double,
      isExplicitDeleted: Boolean,
      orderNum: String,
      comment: String,
      payType: String,
      goodsType: String
  )

  // define Withdraw relationship
  case class WithdrawRaw(
      fromId: String,
      toId: String,
      fromType: String,
      toType: String,
      multiplicityId: Long,
      createTime: Long,
      deleteTime: Long,
      amount: Double,
      isExplicitDeleted: Boolean
  )

  // define Repay relationship
  case class RepayRaw(
      accountId: String,
      loanId: String,
      createTime: Long,
      deleteTime: Long,
      amount: Double,
      isExplicitDeleted: Boolean
  )

  // define Deposit relationship
  case class DepositRaw(
      loanId: String,
      accountId: String,
      createTime: Long,
      deleteTime: Long,
      amount: Double,
      isExplicitDeleted: Boolean
  )

  // define SignIn relationship
  case class SignInRaw(
      mediumId: String,
      accountId: String,
      multiplicityId: Long,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean,
      location: String
  )
}
