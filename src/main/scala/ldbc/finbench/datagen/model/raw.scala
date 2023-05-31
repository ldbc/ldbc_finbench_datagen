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
  )

  // define Account entity
  case class AccountRaw(
      id: Long,
      createTime: Long,
      deleteTime: Long,
      isBlocked: Boolean,
      `type`: String,
      inDegree: Long,
      OutDegree: Long,
      isExplicitDeleted: Boolean,
      Owner: String
  )

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
  )

  // define Loan entity
  case class LoanRaw(
      id: Long,
      createTime: Long,
      loanAmount: Double,
      balance: Double,
      usage: String,
      interestRate: Double
  )

  // define Medium entity
  case class MediumRaw(
      id: Long,
      createTime: Long,
      `type`: String,
      isBlocked: Boolean
  )

  // define PersonApplyLoan relationship
  case class PersonApplyLoanRaw(
      `personId`: Long,
      `loanId`: Long,
      loanAmount: Double,
      createTime: Long
  )

  // define CompanyApplyLoan relationship
  case class CompanyApplyLoanRaw(
      `companyId`: Long,
      `loanId`: Long,
      loanAmount: Double,
      createTime: Long
  )

  // define PersonInvestCompany relationship
  case class PersonInvestCompanyRaw(
      investorId: Long,
      companyId: Long,
      createTime: Long,
      ratio: Double
  )

  // define CompanyInvestCompany relationship
  case class CompanyInvestCompanyRaw(
      investorId: Long,
      companyId: Long,
      createTime: Long,
      ratio: Double
  )

  // define PersonGuaranteePerson relationship
  case class PersonGuaranteePersonRaw(
      fromId: Long,
      toId: Long,
      createTime: Long
  )

  // define CompanyGuarantee relationship
  case class CompanyGuaranteeCompanyRaw(
      fromId: Long,
      toId: Long,
      createTime: Long
  )

  //define PersonOwnAccount relationship
  case class PersonOwnAccountRaw(
      `personId`: Long,
      `accountId`: Long,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean
  )

  // define CompanyOwnAccount relationship
  case class CompanyOwnAccountRaw(
      `companyId`: Long,
      `accountId`: Long,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean
  )

  // define Transfer relationship
  case class TransferRaw(
      fromId: Long,
      toId: Long,
      multiplicityId: Long,
      createTime: Long,
      deleteTime: Long,
      amount: Double,
      isExplicitDeleted: Boolean
  )

  // define Withdraw relationship
  case class WithdrawRaw(
      fromId: Long,
      toId: Long,
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
      `accountId`: Long,
      `loanId`: Long,
      createTime: Long,
      deleteTime: Long,
      amount: Double,
      isExplicitDeleted: Boolean
  )

  // define Deposit relationship
  case class DepositRaw(
      `loanId`: Long,
      `accountId`: Long,
      createTime: Long,
      deleteTime: Long,
      amount: Double,
      isExplicitDeleted: Boolean
  )

  // define SignIn relationship
  case class SignInRaw(
      `mediumId`: Long,
      `accountId`: Long,
      multiplicityId: Long,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean
  )
}
