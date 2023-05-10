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
      isBlocked: Boolean
  )

  // define Account entity
  case class AccountRaw(
      id: Long,
      createTime: Long,
      deleteTime: Long,
      isBlocked: Boolean,
      `type`: String
  )

  // define Company entity
  case class CompanyRaw(
      id: Long,
      createTime: Long,
      name: String,
      isBlocked: Boolean
  )

  // define Loan entity
  case class LoanRaw(
      id: Long,
      loanAmount: Long,
      balance: Long
  )

  // define Medium entity
  case class MediumRaw(
      id: Long,
      createTime: Long,
      name: String,
      isBlocked: Boolean
  )

  // define PersonApplyLoan relationship
  case class PersonApplyLoanRaw(
      `personId`: Long,
      `loanId`: Long,
      timestamp: Long
  )

  // define CompanyApplyLoan relationship
  case class CompanyApplyLoanRaw(
      `companyId`: Long,
      `loanId`: Long,
      timestamp: Long
  )

  // define WorkIn relationship
  case class WorkInRaw(
      `personId`: Long,
      `companyId`: Long
  )

  // define PersonInvestCompany relationship
  case class PersonInvestCompanyRaw(
      `personId`: Long,
      `companyId`: Long,
      timestamp: Long,
      ratio: Float
  )

  // define CompanyInvestCompany relationship
  case class CompanyInvestCompanyRaw(
      `company1Id`: Long,
      `company2Id`: Long,
      timestamp: Long,
      ratio: Float
  )

  // define PersonGuaranteePerson relationship
  case class PersonGuaranteePersonRaw(
      `person1Id`: Long,
      `person2Id`: Long,
      timestamp: Long
  )

  // define CompanyGuarantee relationship
  case class CompanyGuaranteeCompanyRaw(
      `company1Id`: Long,
      `company2Id`: Long,
      timestamp: Long
  )

  //define PersonOwnAccount relationship
  case class PersonOwnAccountRaw(
      `personId`: Long,
      `accountId`: Long,
      createTime: Long,
      deleteTime: Long
  )

  // define CompanyOwnAccount relationship
  case class CompanyOwnAccountRaw(
      `companyId`: Long,
      `accountId`: Long,
      createTime: Long,
      deleteTime: Long
  )

  // define Transfer relationship
  case class TransferRaw(
      `account1Id`: Long,
      `account2Id`: Long,
      timestamp: Long,
      amount: Long,
      `type`: String
  )

  // define Withdraw relationship
  case class WithdrawRaw(
      `account1Id`: Long,
      `account2Id`: Long,
      timestamp: Long,
      amount: Long
  )

  // define Repay relationship
  case class RepayRaw(
      `accountId`: Long,
      `loanId`: Long,
      timestamp: Long,
      amount: Long
  )

  // define Deposit relationship
  case class DepositRaw(
      `loanId`: Long,
      `accountId`: Long,
      timestamp: Long,
      amount: Long
  )

  // define SignIn relationship
  case class SignInRaw(
      `mediumId`: Long,
      `accountId`: Long,
      timestamp: Long
  )
}
