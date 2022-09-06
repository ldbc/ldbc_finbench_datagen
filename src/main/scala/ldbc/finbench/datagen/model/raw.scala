package ldbc.finbench.datagen.model

/**
  * define LDBC Finbench Data Schema
  */
object raw {

  // define Person entity
  case class Person(
      id: Long,
      name: String,
      isBlocked: Boolean
  )

  // define Account entity
  case class Account(
      id: Long,
      createTime: Long,
      isBlocked: Boolean,
      `type`: String
  )

  // define Company entity
  case class Company(
      id: Long,
      name: String,
      isBlocked: Boolean
  )

  // define Loan entity
  case class Loan(
      id: Long,
      loanAmount: Long,
      balance: Long
  )

  // define Medium entity
  case class Medium(
      id: Long,
      name: String,
      isBlocked: Boolean
  )

  // define PersonApplyLoan relationship
  case class PersonApplyLoan(
      `personId`: Long,
      `loanId`: Long
  )

  // define CompanyApplyLoan relationship
  case class CompanyApplyLoan(
      `companyId`: Long,
      `loanId`: Long
  )

  // define WorkIn relationship
  case class WorkIn(
      `personId`: Long,
      `companyId`: Long
  )

  // define PersonInvestCompany relationship
  case class PersonInvestCompany(
      `personId`: Long,
      `companyId`: Long
  )

  // define CompanyInvestCompany relationship
  case class CompanyInvestCompany(
      `company1Id`: Long,
      `company2Id`: Long
  )

  // define PersonGuaranteePerson relationship
  case class PersonGuaranteePerson(
      `person1Id`: Long,
      `person2Id`: Long
  )

  // define CompanyGuarantee relationship
  case class CompanyGuaranteeCompany(
      `company1Id`: Long,
      `company2Id`: Long
  )

  //define PersonOwnAccount relationship
  case class PersonOwnAccount(
      `personId`: Long,
      `accountId`: Long
  )

  // define CompanyOwnAccount relationship
  case class CompanyOwnAccount(
      `companyId`: Long,
      `accountId`: Long
  )

  // define Transfer relationship
  case class Transfer(
      `account1Id`: Long,
      `account2Id`: Long
  )

  // define Withdraw relationship
  case class Withdraw(
      `account1Id`: Long,
      `account2Id`: Long
  )

  // define Repay relationship
  case class Repay(
      `accountId`: Long,
      `loanId`: Long
  )

  // define Deposit relationship
  case class Deposit(
      `loanId`: Long,
      `accountId`: Long
  )

  // define SignIn relationship
  case class SignIn(
      `mediumId`: Long,
      `accountId`: Long
  )
}
