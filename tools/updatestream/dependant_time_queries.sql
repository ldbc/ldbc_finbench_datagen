-- Inserts
--- Insert 1: add a person
COPY (
SELECT
    Person.createTime,
    0 AS dependencyTime,
    Person.id,
    Person.name,
    Person.isBlocked
FROM Person
WHERE Person.createTime > :start_date_long
ORDER BY Person.createTime
)
TO ':output_dir/inserts/AddPerson.parquet' (FORMAT 'parquet');

--- Insert 2: add a company
COPY (
SELECT
    Company.createTime,
    0 AS dependencyTime,
    Company.id,
    Company.name,
    Company.isBlocked
FROM Company
WHERE Company.createTime > :start_date_long
ORDER BY Company.createTime
)
TO ':output_dir/inserts/AddCompany.parquet' (FORMAT 'parquet');


--- Insert 3: person registers account
COPY (
SELECT
    PersonOwnAccount.createTime,
    Person.createTime AS dependencyTime,
    accountId,
    Account.type AS accountType,
    Account.isBlocked AS accountBlocked
FROM Person, Account, PersonOwnAccount
WHERE Person.id == PersonOwnAccount.personId
  AND Account.id == PersonOwnAccount.accountId
  AND PersonOwnAccount.createTime > :start_date_long
ORDER BY PersonOwnAccount.createTime
)
TO ':output_dir/inserts/PersonRegister.parquet' (FORMAT 'parquet');

--- Insert 4: company registers account
COPY (
SELECT
    CompanyOwnAccount.createTime,
    Company.createTime AS dependencyTime,
    accountId,
    Account.type AS accountType,
    Account.isBlocked AS accountBlocked
FROM Company, Account, CompanyOwnAccount
WHERE Company.id == CompanyOwnAccount.companyId
  AND Account.id == CompanyOwnAccount.accountId
  AND CompanyOwnAccount.createTime > :start_date_long
ORDER BY CompanyOwnAccount.createTime
)
TO ':output_dir/inserts/CompanyRegister.parquet' (FORMAT 'parquet');

--- Insert 5: person invests company
COPY (
SELECT
    PersonInvest.createTime,
    GREATEST(Person.createTime, Company.createTime) AS dependencyTime,
    PersonInvest.investorId,
    PersonInvest.companyId,
    PersonInvest.ratio AS ratio
FROM PersonInvest, Person, Company
WHERE PersonInvest.createTime > :start_date_long
    AND Person.id == PersonInvest.investorId
    AND Company.id == PersonInvest.companyId
ORDER BY PersonInvest.createTime
)
TO ':output_dir/inserts/PersonInvest.parquet' (FORMAT 'parquet');

--- Insert 6: company invests company
COPY (
SELECT
    CompanyInvest.createTime,
    GREATEST(Company1.createTime, Company2.createTime) AS dependencyTime,
    CompanyInvest.investorId,
    CompanyInvest.companyId,
    CompanyInvest.ratio AS ratio
FROM CompanyInvest, Company AS Company1, Company AS Company2
WHERE CompanyInvest.createTime > :start_date_long
    AND Company1.id == CompanyInvest.investorId
    AND Company2.id == CompanyInvest.companyId
ORDER BY CompanyInvest.createTime
)
TO ':output_dir/inserts/CompanyInvest.parquet' (FORMAT 'parquet');

--- Insert 7: person guarantees person
COPY (
SELECT
    PersonGuarantee.createTime,
    GREATEST(Person1.createTime, Person2.createTime) AS dependencyTime,
    PersonGuarantee.from,
    PersonGuarantee.to
FROM PersonGuarantee, Person AS Person1, Person AS Person2
WHERE PersonGuarantee.createTime > :start_date_long
    AND Person1.id == PersonGuarantee.from
    AND Person2.id == PersonGuarantee.to
ORDER BY PersonGuarantee.createTime
)
TO ':output_dir/inserts/PersonGuarantee.parquet' (FORMAT 'parquet');

--- Insert 8: company guarantees company
COPY (
SELECT
    CompanyGuarantee.createTime,
    GREATEST(Company1.createTime, Company2.createTime) AS dependencyTime,
    CompanyGuarantee.from,
    CompanyGuarantee.to
FROM CompanyGuarantee, Company AS Company1, Company AS Company2
WHERE CompanyGuarantee.createTime > :start_date_long
    AND Company1.id == CompanyGuarantee.from
    AND Company2.id == CompanyGuarantee.to
ORDER BY CompanyGuarantee.createTime
)
TO ':output_dir/inserts/CompanyGuarantee.parquet' (FORMAT 'parquet');

--- Insert 9: person applies loan
COPY (
SELECT
    PersonApplyLoan.createTime,
    Person.createTime AS dependencyTime,
    Loan.loanAmount,
    Loan.balance,
FROM PersonApplyLoan, Person, Loan
WHERE PersonApplyLoan.createTime > :start_date_long
    AND PersonApplyLoan.personId == Person.id
    AND PersonApplyLoan.loanId == Loan.id
ORDER BY PersonApplyLoan.createTime
)
TO ':output_dir/inserts/PersonApplyLoan.parquet' (FORMAT 'parquet');

--- Insert 10: company applies loan
COPY (
SELECT
    CompanyApplyLoan.createTime,
    Company.createTime AS dependencyTime,
    Loan.loanAmount,
    Loan.balance,
FROM CompanyApplyLoan, Company, Loan
WHERE CompanyApplyLoan.createTime > :start_date_long
    AND CompanyApplyLoan.companyId == Company.id
    AND CompanyApplyLoan.loanId == Loan.id
ORDER BY CompanyApplyLoan.createTime
)
TO ':output_dir/inserts/CompanyApplyLoan.parquet' (FORMAT 'parquet');

--- Insert 11: Add medium
COPY (
SELECT
    Medium.createTime,
    0 AS dependencyTime,
    Medium.id,
    Medium.type,
    Medium.isBlocked
FROM Medium
WHERE Medium.createTime > :start_date_long
ORDER BY Medium.createTime
    )
TO ':output_dir/inserts/Medium.parquet' (FORMAT 'parquet');

--- Insert 12: medium signs in account
COPY (
SELECT
    SignIn.createTime,
    GREATEST(Account.createTime, Medium.createTime) AS dependencyTime,
    SignIn.mediumId,
    SignIn.accountId
FROM Medium, SignIn, Account
WHERE SignIn.createTime > :start_date_long
    AND Medium.id == SignIn.mediumId
    AND Account.id == SignIn.accountId
ORDER BY SignIn.createTime
)
TO ':output_dir/inserts/SignIn.parquet' (FORMAT 'parquet');

--- Insert 13: transfer
COPY (
(
SELECT
    Transfer.createTime AS createTime,
    GREATEST(Acc1.createTime, Acc2.createTime) AS dependencyTime,
    Transfer.fromId,
    Transfer.toId,
    Transfer.amount
FROM Transfer, Account AS Acc1, Account AS Acc2
WHERE Transfer.createTime > :start_date_long
    AND Acc1.id == Transfer.fromId
    AND Acc2.id == Transfer.toId
)
UNION ALL
(
SELECT
    LoanTransfer.createTime AS createTime,
    GREATEST(Acc1.createTime, Acc2.createTime) AS dependencyTime,
    LoanTransfer.fromId,
    LoanTransfer.toId,
    LoanTransfer.amount
FROM LoanTransfer, Account AS Acc1, Account AS Acc2
WHERE LoanTransfer.createTime > :start_date_long
    AND Acc1.id == LoanTransfer.fromId
    AND Acc2.id == LoanTransfer.toId
)
ORDER BY createTime
)
TO ':output_dir/inserts/Transfer.parquet' (FORMAT 'parquet');

--- Insert 14: withdraw
COPY (
SELECT
    Withdraw.createTime,
    GREATEST(Acc1.createTime, Acc2.createTime) AS dependencyTime,
    Withdraw.fromId,
    Withdraw.toId,
    Withdraw.amount
FROM Withdraw, Account AS Acc1, Account AS Acc2
WHERE Withdraw.createTime > :start_date_long
    AND Acc1.id == Withdraw.fromId
    AND Acc2.id == Withdraw.toId
ORDER BY Withdraw.createTime
)
TO ':output_dir/inserts/Withdraw.parquet' (FORMAT 'parquet');

--- Insert 15: Deposits
COPY (
SELECT
    Deposit.createTime,
    GREATEST(Account.createTime, Loan.createTime) AS dependencyTime,
    Deposit.accountId,
    Deposit.loanId,
    Deposit.amount
FROM Deposit, Account, Loan
WHERE Deposit.createTime > :start_date_long
    AND Account.id == Deposit.accountId
    AND Loan.id == Deposit.loanId
ORDER BY Deposit.createTime
)
TO ':output_dir/inserts/Deposit.parquet' (FORMAT 'parquet');

--- Insert 16: Deposits
COPY (
SELECT
    Repay.createTime,
    GREATEST(Account.createTime, Loan.createTime) AS dependencyTime,
    Repay.accountId,
    Repay.loanId,
    Repay.amount
FROM Repay, Account, Loan
WHERE Repay.createTime > :start_date_long
    AND Account.id == Repay.accountId
    AND Loan.id == Repay.loanId
ORDER BY Repay.createTime
)
TO ':output_dir/inserts/Repay.parquet' (FORMAT 'parquet');


-- Delete 1: delete an account
COPY (SELECT deleteTime, createTime as dependentDate, id
      FROM Account
      WHERE deleteTime > :start_date_long
        AND isExplicitDeleted = true
      ORDER BY deleteTime ASC)
TO ':output_dir/deletes/Account.parquet' (FORMAT 'parquet');
