
--- Person
COPY (
SELECT id, name, isBlocked, createTime
FROM Person
WHERE Person.createTime <= :start_date_long
ORDER BY Person.createTime
)
TO ':output_dir/snapshot/Person.:output_format';

--- Company
COPY (
SELECT id, name, isBlocked, createTime
FROM Company
WHERE Company.createTime <= :start_date_long
ORDER BY Company.createTime
)
TO ':output_dir/snapshot/Company.:output_format';

-- Account. It has deletes.
COPY (
SELECT id, createTime, isBlocked, type, createTime
FROM Account
WHERE Account.createTime <= :start_date_long AND Account.deleteTime > :start_date_long
ORDER BY Account.createTime
)
TO ':output_dir/snapshot/Account.:output_format';

-- Loan.
COPY (
SELECT id, loanAmount, balance, createTime
FROM Loan
WHERE Loan.createTime <= :start_date_long
ORDER BY Loan.createTime
    )
TO ':output_dir/snapshot/Loan.:output_format';

-- Medium.
COPY (
SELECT id, type, isBlocked, createTime
FROM Medium
WHERE Medium.createTime <= :start_date_long
ORDER BY Medium.createTime
)
TO ':output_dir/snapshot/Medium.:output_format';

-- Transfer. It has deletes.
COPY (
SELECT fromId, toId, createTime, amount
FROM Transfer
WHERE Transfer.createTime <= :start_date_long AND Transfer.deleteTime > :start_date_long
ORDER BY Transfer.createTime
)
TO ':output_dir/snapshot/Transfer.:output_format';

-- Withdraw. It has deletes.
COPY (
SELECT fromId, toId, createTime, amount
FROM Withdraw
WHERE Withdraw.createTime <= :start_date_long AND Withdraw.deleteTime > :start_date_long
ORDER BY Withdraw.createTime
)
TO ':output_dir/snapshot/Withdraw.:output_format';

-- Repay. It has deletes.
COPY (
SELECT accountId, loanId, createTime, amount
FROM Repay
WHERE Repay.createTime <= :start_date_long AND Repay.deleteTime > :start_date_long
ORDER BY Repay.createTime
)
TO ':output_dir/snapshot/Repay.:output_format';

-- Deposit. It has deletes.
COPY (
SELECT loanId, accountId, createTime, amount
FROM Deposit
WHERE Deposit.createTime <= :start_date_long AND Deposit.deleteTime > :start_date_long
ORDER BY Deposit.createTime
)
TO ':output_dir/snapshot/Deposit.:output_format';

-- SignIn. It has deletes.
COPY (
SELECT mediumId, accountId, createTime
FROM SignIn
WHERE SignIn.createTime <= :start_date_long AND SignIn.deleteTime > :start_date_long
ORDER BY SignIn.createTime
)
TO ':output_dir/snapshot/MediumSignInAccount.:output_format';

-- PersonInvest.
COPY (
SELECT investorId, companyId, createTime, ratio
FROM PersonInvest
WHERE PersonInvest.createTime <= :start_date_long
ORDER BY PersonInvest.createTime
)
TO ':output_dir/snapshot/PersonInvestCompany.:output_format';

-- CompanyInvest.
COPY (
SELECT investorId, companyId, createTime, ratio
FROM CompanyInvest
WHERE CompanyInvest.createTime <= :start_date_long
ORDER BY CompanyInvest.createTime
)
TO ':output_dir/snapshot/CompanyInvestCompany.:output_format';

-- PersonApplyLoan.
COPY (
SELECT personId, loanId, createTime
FROM PersonApplyLoan
WHERE PersonApplyLoan.createTime <= :start_date_long
ORDER BY PersonApplyLoan.createTime
)
TO ':output_dir/snapshot/PersonApplyLoan.:output_format';

-- CompanyApplyLoan.
COPY (
SELECT companyId, loanId, createTime
FROM CompanyApplyLoan
WHERE CompanyApplyLoan.createTime <= :start_date_long
ORDER BY CompanyApplyLoan.createTime
)
TO ':output_dir/snapshot/CompanyApplyLoan.:output_format';

-- PersonGuaranteePerson.
COPY (
SELECT fromId, toId, createTime
FROM PersonGuarantee
WHERE PersonGuarantee.createTime <= :start_date_long
ORDER BY PersonGuarantee.createTime
)
TO ':output_dir/snapshot/PersonGuaranteePerson.:output_format';

-- CompanyGuaranteeCompany.
COPY (
SELECT fromId, toId, createTime
FROM CompanyGuarantee
WHERE CompanyGuarantee.createTime <= :start_date_long
ORDER BY CompanyGuarantee.createTime
)
TO ':output_dir/snapshot/CompanyGuaranteeCompany.:output_format';

-- PersonOwnAccount. It has deletes.
COPY (
SELECT personId, accountId, createTime
FROM PersonOwnAccount
WHERE PersonOwnAccount.createTime <= :start_date_long AND PersonOwnAccount.deleteTime > :start_date_long
ORDER BY PersonOwnAccount.createTime
)
TO ':output_dir/snapshot/PersonOwnAccount.:output_format';

-- CompanyOwnAccount. It has deletes.
COPY (
SELECT companyId, accountId, createTime
FROM CompanyOwnAccount
WHERE CompanyOwnAccount.createTime <= :start_date_long AND CompanyOwnAccount.deleteTime > :start_date_long
ORDER BY CompanyOwnAccount.createTime
)
TO ':output_dir/snapshot/CompanyOwnAccount.:output_format';
