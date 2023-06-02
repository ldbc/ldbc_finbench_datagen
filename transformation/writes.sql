-- Inserts
--- Insert 1: add a person
COPY
(
SELECT Person.createTime AS createTime,
       0                 AS dependencyTime,
       Person.id         AS personId,
       Person.name       AS personName,
       Person.isBlocked  AS isBlocked,
       Person.gender     AS gender,
       Person.birthday   AS birthday,
       Person.country    AS country,
       Person.city       AS city
FROM Person
WHERE Person.createTime > :start_date_long
ORDER BY Person.createTime )
TO ':output_dir/incremental/AddPersonWrite1.:output_format';

--- Insert 2: add a company
COPY
(
SELECT Company.createTime  AS createTime,
       0                   AS dependencyTime,
       Company.id          AS companyId,
       Company.name        AS companyName,
       Company.isBlocked   AS isBlocked,
       Company.country     AS country,
       Company.city        AS city,
       Company.business    AS business,
       Company.description AS description,
       Company.url         AS url
FROM Company
WHERE Company.createTime > :start_date_long
ORDER BY Company.createTime )
TO ':output_dir/incremental/AddCompanyWrite2.:output_format';

--- Insert 3: Add medium
COPY
(
SELECT Medium.createTime AS createTime,
       0                 AS dependencyTime,
       Medium.id         AS mediumId,
       Medium.type       AS mediumType,
       Medium.isBlocked  AS isBlocked,
       Medium.lastLogin  AS lastLoginTime,
       Medium.riskLevel  AS riskLevel
FROM Medium
WHERE Medium.createTime > :start_date_long
ORDER BY Medium.createTime )
TO ':output_dir/incremental/AddMediumWrite3.:output_format';


--- Insert 4: person owns account
COPY
(
SELECT PersonOwnAccount.createTime AS createTime,
       Person.createTime           AS dependencyTime,
       PersonOwnAccount.personId   AS personId,
       PersonOwnAccount.accountId  AS accountId,
       Account.type                AS accountType,
       Account.isBlocked           AS accountBlocked,
       Account.nickname            AS nickname,
       Account.phonenum            AS phonenum,
       Account.email               AS email,
       Account.freqLoginType       AS freqLoginType,
       Account.lastLoginTime       AS lastLoginTime,
       Account.accountLevel        AS accountLevel,
FROM Person,
     Account,
     PersonOwnAccount
WHERE Person.id == PersonOwnAccount.personId
  AND Account.id == PersonOwnAccount.accountId
  AND PersonOwnAccount.createTime > :start_date_long
ORDER BY PersonOwnAccount.createTime
    )
    TO ':output_dir/incremental/AddPersonOwnAccountWrite4.:output_format';

--- Insert 5: company owns account
COPY
(
SELECT CompanyOwnAccount.createTime AS createTime,
       Company.createTime           AS dependencyTime,
       CompanyOwnAccount.companyId  AS companyId,
       CompanyOwnAccount.accountId  AS accountId,
       Account.type                 AS accountType,
       Account.isBlocked            AS accountBlocked,
       Account.nickname             AS nickname,
       Account.phonenum             AS phonenum,
       Account.email                AS email,
       Account.freqLoginType        AS freqLoginType,
       Account.lastLoginTime        AS lastLoginTime,
       Account.accountLevel         AS accountLevel,
FROM Company,
     Account,
     CompanyOwnAccount
WHERE Company.id == CompanyOwnAccount.companyId
  AND Account.id == CompanyOwnAccount.accountId
  AND CompanyOwnAccount.createTime > :start_date_long
ORDER BY CompanyOwnAccount.createTime
    )
    TO ':output_dir/incremental/AddCompanyOwnAccountWrite5.:output_format';

--- Insert 6: person applies loan
COPY
(
SELECT PersonApplyLoan.createTime AS createTime,
       Person.createTime          AS dependencyTime,
       PersonApplyLoan.personId   AS personId,
       Loan.id                    AS loanId,
       Loan.loanAmount            AS loanAmount,
       Loan.balance               AS balance,
       Loan.usage                 AS loanUsage,
       Loan.interestRate          AS interestRate,
       PersonApplyLoan.org        AS org
FROM PersonApplyLoan,
     Person,
     Loan
WHERE PersonApplyLoan.createTime > :start_date_long
  AND PersonApplyLoan.personId == Person.id
    AND PersonApplyLoan.loanId == Loan.id
ORDER BY PersonApplyLoan.createTime
    )
    TO ':output_dir/incremental/AddPersonApplyLoanWrite6.:output_format';

--- Insert 7: company applies loan
COPY
(
SELECT CompanyApplyLoan.createTime AS createTime,
       Company.createTime          AS dependencyTime,
       CompanyApplyLoan.companyId  AS companyId,
       Loan.id                     AS loanId,
       Loan.loanAmount             AS loanAmount,
       Loan.balance                AS balance,
       Loan.usage                  AS loanUsage,
       Loan.interestRate           AS interestRate,
       CompanyApplyLoan.org        AS org
FROM CompanyApplyLoan,
     Company,
     Loan
WHERE CompanyApplyLoan.createTime > :start_date_long
  AND CompanyApplyLoan.companyId == Company.id
    AND CompanyApplyLoan.loanId == Loan.id
ORDER BY CompanyApplyLoan.createTime
    )
    TO ':output_dir/incremental/AddCompanyApplyLoanWrite7.:output_format';

--- Insert 8: person invests company
COPY
(
SELECT PersonInvest.createTime                         AS createTime,
       GREATEST(Person.createTime, Company.createTime) AS dependencyTime,
       PersonInvest.investorId                         AS investorId,
       PersonInvest.companyId                          AS companyId,
       PersonInvest.ratio                              AS ratio
FROM PersonInvest,
     Person,
     Company
WHERE PersonInvest.createTime > :start_date_long
  AND Person.id == PersonInvest.investorId
    AND Company.id == PersonInvest.companyId
ORDER BY PersonInvest.createTime
    )
    TO ':output_dir/incremental/AddPersonInvestCompanyWrite8.:output_format';

--- Insert 9: company invests company
COPY
(
SELECT CompanyInvest.createTime                           AS createTime,
       GREATEST(Company1.createTime, Company2.createTime) AS dependencyTime,
       CompanyInvest.investorId                           AS investorId,
       CompanyInvest.companyId                            AS companyId,
       CompanyInvest.ratio                                AS ratio
FROM CompanyInvest,
     Company AS Company1,
     Company AS Company2
WHERE CompanyInvest.createTime > :start_date_long
  AND Company1.id == CompanyInvest.investorId
    AND Company2.id == CompanyInvest.companyId
ORDER BY CompanyInvest.createTime
    )
    TO ':output_dir/incremental/AddCompanyInvestCompanyWrite9.:output_format';

--- Insert 10: person guarantees person
COPY
(
SELECT PersonGuarantee.createTime                       AS createTime,
       GREATEST(Person1.createTime, Person2.createTime) AS dependencyTime,
       PersonGuarantee.fromId                           AS fromId,
       PersonGuarantee.toId                             AS toId,
       PersonGuarantee.relation                         AS relation
FROM PersonGuarantee,
     Person AS Person1,
     Person AS Person2
WHERE PersonGuarantee.createTime > :start_date_long
  AND Person1.id == PersonGuarantee.fromId
    AND Person2.id == PersonGuarantee.toId
ORDER BY PersonGuarantee.createTime
    )
    TO ':output_dir/incremental/AddPersonGuaranteePersonAll.:output_format';

--- Insert 11: company guarantees company
COPY
(
SELECT CompanyGuarantee.createTime                        AS createTime,
       GREATEST(Company1.createTime, Company2.createTime) AS dependencyTime,
       CompanyGuarantee.fromId                            AS fromId,
       CompanyGuarantee.toId                              AS toId,
       CompanyGuarantee.relation                          AS relation
FROM CompanyGuarantee,
     Company AS Company1,
     Company AS Company2
WHERE CompanyGuarantee.createTime > :start_date_long
  AND Company1.id == CompanyGuarantee.fromId
    AND Company2.id == CompanyGuarantee.toId
ORDER BY CompanyGuarantee.createTime
    )
    TO ':output_dir/incremental/AddCompanyGuaranteeCompanyWrite11.:output_format';

--- Insert 12: transfer
COPY
(
(
SELECT
    Transfer.createTime                        AS createTime,
    GREATEST(Acc1.createTime, Acc2.createTime) AS dependencyTime,
    Transfer.fromId                            AS fromId,
    Transfer.toId                              AS toId,
    Transfer.amount                            AS amount,
    Transfer.orderNum                          AS orderNum,
    Transfer.comment                           AS comment,
    Transfer.payType                           AS payType,
    Transfer.goodsType                         AS goodsType
FROM Transfer, Account AS Acc1, Account AS Acc2
WHERE Transfer.createTime > :start_date_long
    AND Acc1.id == Transfer.fromId
    AND Acc2.id == Transfer.toId
)
UNION ALL
(
SELECT
    LoanTransfer.createTime                    AS createTime,
    GREATEST(Acc1.createTime, Acc2.createTime) AS dependencyTime,
    LoanTransfer.fromId                        AS fromId,
    LoanTransfer.toId                          AS toId,
    LoanTransfer.amount                        AS amount,
    LoanTransfer.orderNum                      AS orderNum,
    LoanTransfer.comment                       AS comment,
    LoanTransfer.payType                       AS payType,
    LoanTransfer.goodsType                     AS goodsType
FROM LoanTransfer, Account AS Acc1, Account AS Acc2
WHERE LoanTransfer.createTime > :start_date_long
    AND Acc1.id == LoanTransfer.fromId
    AND Acc2.id == LoanTransfer.toId
)
ORDER BY createTime
)
TO ':output_dir/incremental/AddAccountTransferAccountAll.:output_format';

--- Insert 13: withdraw
COPY
(
SELECT Withdraw.createTime                        AS createTime,
       GREATEST(Acc1.createTime, Acc2.createTime) AS dependencyTime,
       Withdraw.fromId                            AS fromId,
       Withdraw.toId                              AS toId,
       Withdraw.amount                            AS amount
FROM Withdraw,
     Account AS Acc1,
     Account AS Acc2
WHERE Withdraw.createTime > :start_date_long
  AND Acc1.id == Withdraw.fromId
    AND Acc2.id == Withdraw.toId
ORDER BY Withdraw.createTime
    )
    TO ':output_dir/incremental/AddAccountWithdrawAccountWrite13.:output_format';

--- Insert 14: Deposits
COPY
(
SELECT Repay.createTime                              AS createTime,
       GREATEST(Account.createTime, Loan.createTime) AS dependencyTime,
       Repay.accountId                               AS account,
       Repay.loanId                                  AS loanId,
       Repay.amount                                  AS amount
FROM Repay,
     Account,
     Loan
WHERE Repay.createTime > :start_date_long
  AND Account.id == Repay.accountId
    AND Loan.id == Repay.loanId
ORDER BY Repay.createTime
    )
    TO ':output_dir/incremental/AddAccountRepayLoanWrite14.:output_format';

--- Insert 15: Deposits
COPY
(
SELECT Deposit.createTime                            AS createTime,
       GREATEST(Account.createTime, Loan.createTime) AS dependencyTime,
       Deposit.accountId                             AS accountId,
       Deposit.loanId                                AS loanId,
       Deposit.amount                                AS amount
FROM Deposit,
     Account,
     Loan
WHERE Deposit.createTime > :start_date_long
  AND Account.id == Deposit.accountId
    AND Loan.id == Deposit.loanId
ORDER BY Deposit.createTime
    )
    TO ':output_dir/incremental/AddLoanDepositAccountWrite15.:output_format';

--- Insert 16: medium signs in account
COPY
(
SELECT SignIn.createTime                               AS createTime,
       GREATEST(Account.createTime, Medium.createTime) AS dependencyTime,
       SignIn.mediumId                                 AS mediumId,
       SignIn.accountId                                AS accountId,
       SignIn.location                                 AS location
FROM Medium,
     SignIn,
     Account
WHERE SignIn.createTime > :start_date_long
  AND Medium.id == SignIn.mediumId
    AND Account.id == SignIn.accountId
ORDER BY SignIn.createTime
    )
    TO ':output_dir/incremental/AddMediumSigninAccountWrite16.:output_format';

-- Delete 17: delete an account
COPY
(
SELECT deleteTime, createTime AS dependentDate, id AS accountId
FROM Account
WHERE deleteTime > :start_date_long
  AND isExplicitDeleted = true
ORDER BY deleteTime ASC)
TO ':output_dir/incremental/DeleteAccountWrite17.:output_format';
