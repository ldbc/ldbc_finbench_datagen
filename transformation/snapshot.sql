--- Person
COPY
(
SELECT Person.id                   AS personId,
       Person.name                 AS personName,
       Person.isBlocked            AS isBlocked,
       strftime(epoch_ms(Person.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       Person.gender               AS gender,
       epoch_ms(Person.birthday)::DATE   AS birthday,
       Person.country              AS country,
       Person.city                 AS city
FROM Person
WHERE Person.createTime <= :start_date_long
ORDER BY Person.createTime )
TO ':output_dir/snapshot/Person.:output_format';

--- Company
COPY
(
SELECT Company.id                   AS companyId,
       Company.name                 AS companyName,
       Company.isBlocked            AS isBlocked,
       strftime(epoch_ms(Company.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       Company.country              AS country,
       Company.city                 AS city,
       Company.business             AS business,
       Company.description          AS description,
       Company.url                  AS url
FROM Company
WHERE Company.createTime <= :start_date_long
ORDER BY Company.createTime )
TO ':output_dir/snapshot/Company.:output_format';

-- Account. It has deletes.
COPY
(
SELECT Account.id                       AS accountId,
       strftime(epoch_ms(Account.createTime), '%Y-%m-%d %H:%M:%S.%g')     AS createTime,
       Account.isBlocked                AS isBlocked,
       Account.type                     AS accountType,
       Account.nickname                 AS nickname,
       Account.phonenum                 AS phonenum,
       Account.email                    AS email,
       Account.freqLoginType            AS freqLoginType,
       strftime(epoch_ms(Account.lastLoginTime), '%Y-%m-%d %H:%M:%S.%g')  AS lastLoginTime,
       Account.accountLevel             AS accountLevel,
FROM Account
WHERE Account.createTime <= :start_date_long
  -- AND Account.deleteTime > :start_date_long
ORDER BY Account.createTime )
TO ':output_dir/snapshot/Account.:output_format';

-- Loan.
COPY
(
SELECT Loan.id                   AS loanId,
       Loan.loanAmount           AS loanAmount,
       Loan.balance              AS balance,
       strftime(epoch_ms(Loan.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       Loan.usage                AS loanUsage,
       Loan.interestRate         AS interestRate
FROM Loan
WHERE Loan.createTime <= :start_date_long
ORDER BY Loan.createTime )
    TO ':output_dir/snapshot/Loan.:output_format';

-- Medium.
COPY
(
SELECT Medium.id                   AS mediumId,
       Medium.type                 AS mediumType,
       Medium.isBlocked            AS isBlocked,
       strftime(epoch_ms(Medium.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       strftime(epoch_ms(Medium.lastLogin), '%Y-%m-%d %H:%M:%S.%g')  AS lastLoginTime,
       Medium.riskLevel            AS riskLevel
FROM Medium
WHERE Medium.createTime <= :start_date_long
ORDER BY Medium.createTime )
TO ':output_dir/snapshot/Medium.:output_format';

-- Transfer. It has deletes.
COPY
(
    (SELECT
        Transfer.fromId AS fromId,
        Transfer.toId AS toId,
        Transfer.amount AS amount,
        strftime(epoch_ms(Transfer.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
        Transfer.orderNum::VARCHAR AS orderNum,
        Transfer.comment AS comment,
        Transfer.payType AS payType,
        Transfer.goodsType AS goodsType
    FROM Transfer
    WHERE Transfer.createTime <= :start_date_long
      -- AND Transfer.deleteTime > :start_date_long
    )
    UNION ALL
    (SELECT
        LoanTransfer.fromId AS fromId,
        LoanTransfer.toId AS toId,
        LoanTransfer.amount AS amount,
        strftime(epoch_ms(LoanTransfer.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
        LoanTransfer.orderNum::VARCHAR AS orderNum,
        LoanTransfer.comment AS comment,
        LoanTransfer.payType AS payType,
        LoanTransfer.goodsType AS goodsType
    FROM LoanTransfer
    WHERE LoanTransfer.createTime <= :start_date_long
      -- AND LoanTransfer.deleteTime > :start_date_long
    )
    ORDER BY createTime
)
TO ':output_dir/snapshot/AccountTransferAccount.:output_format';

-- Withdraw. It has deletes.
COPY
(
SELECT Withdraw.fromId               AS fromId,
       Withdraw.toId                 AS toId,
       Withdraw.fromType             AS fromType,
       Withdraw.toType               AS toType,
       Withdraw.amount               AS amount,
       strftime(epoch_ms(Withdraw.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       Withdraw.comment              AS comment
FROM Withdraw
WHERE Withdraw.createTime <= :start_date_long
  -- AND Withdraw.deleteTime > :start_date_long
ORDER BY Withdraw.createTime )
TO ':output_dir/snapshot/AccountWithdrawAccount.:output_format';

-- Repay. It has deletes.
COPY
(
SELECT Repay.accountId            AS accountId,
       Repay.loanId               AS loanId,
       Repay.amount               AS amount,
       strftime(epoch_ms(Repay.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       Repay.comment              AS comment
FROM Repay
WHERE Repay.createTime <= :start_date_long
  -- AND Repay.deleteTime > :start_date_long
ORDER BY Repay.createTime )
TO ':output_dir/snapshot/AccountRepayLoan.:output_format';

-- Deposit. It has deletes.
COPY
(
SELECT Deposit.loanId               AS loanId,
       Deposit.accountId            AS accountId,
       Deposit.amount               AS amount,
       strftime(epoch_ms(Deposit.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       Deposit.comment              AS comment
FROM Deposit
WHERE Deposit.createTime <= :start_date_long
  -- AND Deposit.deleteTime > :start_date_long
ORDER BY Deposit.createTime )
TO ':output_dir/snapshot/LoanDepositAccount.:output_format';

-- SignIn. It has deletes.
COPY
(
SELECT SignIn.mediumId             AS mediumId,
       SignIn.accountId            AS accountId,
       strftime(epoch_ms(SignIn.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       SignIn.location             AS location,
       SignIn.comment              AS comment
FROM SignIn
WHERE SignIn.createTime <= :start_date_long
  -- AND SignIn.deleteTime > :start_date_long
ORDER BY SignIn.createTime )
TO ':output_dir/snapshot/MediumSignInAccount.:output_format';

-- PersonInvest.
COPY
(
SELECT PersonInvest.investorId           AS investorId,
       PersonInvest.companyId            AS companyId,
       PersonInvest.ratio                AS ratio,
       strftime(epoch_ms(PersonInvest.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       PersonInvest.comment              AS comment
FROM PersonInvest
WHERE PersonInvest.createTime <= :start_date_long
ORDER BY PersonInvest.createTime )
TO ':output_dir/snapshot/PersonInvestCompany.:output_format';

-- CompanyInvest.
COPY
(
SELECT CompanyInvest.investorId           AS investorId,
       CompanyInvest.companyId            AS companyId,
       CompanyInvest.ratio                AS ratio,
       strftime(epoch_ms(CompanyInvest.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       CompanyInvest.comment              AS comment
FROM CompanyInvest
WHERE CompanyInvest.createTime <= :start_date_long
ORDER BY CompanyInvest.createTime )
TO ':output_dir/snapshot/CompanyInvestCompany.:output_format';

-- PersonApplyLoan.
COPY
(
SELECT PersonApplyLoan.personId             AS personId,
       PersonApplyLoan.loanId               AS loanId,
       PersonApplyLoan.loanAmount           AS loanAmount,
       strftime(epoch_ms(PersonApplyLoan.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       PersonApplyLoan.org                  AS org,
       PersonApplyLoan.comment              AS comment
FROM PersonApplyLoan
WHERE PersonApplyLoan.createTime <= :start_date_long
ORDER BY PersonApplyLoan.createTime )
TO ':output_dir/snapshot/PersonApplyLoan.:output_format';

-- CompanyApplyLoan.
COPY
(
SELECT CompanyApplyLoan.companyId            AS companyId,
       CompanyApplyLoan.loanId               AS loanId,
       CompanyApplyLoan.loanAmount           AS loanAmount,
       strftime(epoch_ms(CompanyApplyLoan.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       CompanyApplyLoan.org                  AS org,
       CompanyApplyLoan.comment              AS comment
FROM CompanyApplyLoan
WHERE CompanyApplyLoan.createTime <= :start_date_long
ORDER BY CompanyApplyLoan.createTime )
TO ':output_dir/snapshot/CompanyApplyLoan.:output_format';

-- PersonGuaranteePerson.
COPY
(
SELECT PersonGuarantee.fromId               AS fromId,
       PersonGuarantee.toId                 AS toId,
       strftime(epoch_ms(PersonGuarantee.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       PersonGuarantee.relation             AS relation,
       PersonGuarantee.comment              AS comment
FROM PersonGuarantee
WHERE PersonGuarantee.createTime <= :start_date_long
ORDER BY PersonGuarantee.createTime )
TO ':output_dir/snapshot/PersonGuaranteePerson.:output_format';

-- CompanyGuaranteeCompany.
COPY
(
SELECT CompanyGuarantee.fromId               AS fromId,
       CompanyGuarantee.toId                 AS toId,
       strftime(epoch_ms(CompanyGuarantee.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       CompanyGuarantee.relation             AS relation,
       CompanyGuarantee.comment              AS comment
FROM CompanyGuarantee
WHERE CompanyGuarantee.createTime <= :start_date_long
ORDER BY CompanyGuarantee.createTime )
TO ':output_dir/snapshot/CompanyGuaranteeCompany.:output_format';

-- PersonOwnAccount. It has deletes.
COPY
(
SELECT PersonOwnAccount.personId             AS personId,
       PersonOwnAccount.accountId            AS accountId,
       strftime(epoch_ms(PersonOwnAccount.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       PersonOwnAccount.comment              AS comment
FROM PersonOwnAccount
WHERE PersonOwnAccount.createTime <= :start_date_long
  -- AND PersonOwnAccount.deleteTime > :start_date_long
ORDER BY PersonOwnAccount.createTime )
TO ':output_dir/snapshot/PersonOwnAccount.:output_format';

-- CompanyOwnAccount. It has deletes.
COPY
(
SELECT CompanyOwnAccount.companyId            AS companyId,
       CompanyOwnAccount.accountId            AS accountId,
       strftime(epoch_ms(CompanyOwnAccount.createTime), '%Y-%m-%d %H:%M:%S.%g') AS createTime,
       CompanyOwnAccount.comment              AS comment
FROM CompanyOwnAccount
WHERE CompanyOwnAccount.createTime <= :start_date_long
  -- AND CompanyOwnAccount.deleteTime > :start_date_long
ORDER BY CompanyOwnAccount.createTime )
TO ':output_dir/snapshot/CompanyOwnAccount.:output_format';
