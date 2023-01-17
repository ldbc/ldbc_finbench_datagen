package ldbc.finbench.datagen.model

/**
  * define LDBC Finbench Data Schema
  */
object raw {

  // define Person entity
  case class PersonRaw(
      id: Long,
      name: String,
      isBlocked: Boolean
  )

  // define Account entity
  case class AccountRaw(
      id: Long,
      createTime: Long,
      isBlocked: Boolean,
      `type`: String
  )

  // define Company entity
  case class CompanyRaw(
      id: Long,
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
      name: String,
      isBlocked: Boolean
  )

  // define PersonApplyLoan relationship
  case class PersonApplyLoanRaw(
      `personId`: Long,
      `loanId`: Long
  )

  // define CompanyApplyLoan relationship
  case class CompanyApplyLoanRaw(
      `companyId`: Long,
      `loanId`: Long
  )

  // define WorkIn relationship
  case class WorkInRaw(
      `personId`: Long,
      `companyId`: Long
  )

  // define PersonInvestCompany relationship
  case class PersonInvestCompanyRaw(
      `personId`: Long,
      `companyId`: Long
  )

  // define CompanyInvestCompany relationship
  case class CompanyInvestCompanyRaw(
      `company1Id`: Long,
      `company2Id`: Long
  )

  // define PersonGuaranteePerson relationship
  case class PersonGuaranteePersonRaw(
      `person1Id`: Long,
      `person2Id`: Long
  )

  // define CompanyGuarantee relationship
  case class CompanyGuaranteeCompanyRaw(
      `company1Id`: Long,
      `company2Id`: Long
  )

  //define PersonOwnAccount relationship
  case class PersonOwnAccountRaw(
      `personId`: Long,
      `accountId`: Long
  )

  // define CompanyOwnAccount relationship
  case class CompanyOwnAccountRaw(
      `companyId`: Long,
      `accountId`: Long
  )

  // define Transfer relationship
  case class TransferRaw(
      `account1Id`: Long,
      `account2Id`: Long
  )

  // define Withdraw relationship
  case class WithdrawRaw(
      `account1Id`: Long,
      `account2Id`: Long
  )

  // define Repay relationship
  case class RepayRaw(
      `accountId`: Long,
      `loanId`: Long
  )

  // define Deposit relationship
  case class DepositRaw(
      `loanId`: Long,
      `accountId`: Long
  )

  // define SignIn relationship
  case class SignInRaw(
      `mediumId`: Long,
      `accountId`: Long
  )
}
