package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class Deposit implements DynamicActivity, Serializable {
    private long loanId;
    private long loanAmount;
    private long loanBalance;
    private long accountId;
    private String accountType;
    private long accountCreationDate;
    private boolean accountIsBlocked;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public Deposit(long loanId, long loanAmount, long loanBalance, long accountId,
                   String accountType, long accountCreationDate, boolean accountIsBlocked,
                   long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.loanId = loanId;
        this.loanAmount = loanAmount;
        this.loanBalance = loanBalance;
        this.accountId = accountId;
        this.accountType = accountType;
        this.accountCreationDate = accountCreationDate;
        this.accountIsBlocked = accountIsBlocked;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Deposit createDeposit(Random random, Loan loan, Account account) {
        long creationDate = Dictionaries.dates.randomLoanToAccountDate(random, loan, account);

        Deposit deposit = new Deposit(loan.getLoanId(), loan.getLoanAmount(), loan.getBalance(),
                account.getAccountId(), account.getType(), account.getCreationDate(), account.isBlocked(),
                creationDate, 0, false);
        loan.getDeposits().add(deposit);

        return deposit;
    }

    public long getLoanId() {
        return loanId;
    }

    public void setLoanId(long loanId) {
        this.loanId = loanId;
    }

    public long getLoanAmount() {
        return loanAmount;
    }

    public void setLoanAmount(long loanAmount) {
        this.loanAmount = loanAmount;
    }

    public long getLoanBalance() {
        return loanBalance;
    }

    public void setLoanBalance(long loanBalance) {
        this.loanBalance = loanBalance;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    public String getAccountType() {
        return accountType;
    }

    public void setAccountType(String accountType) {
        this.accountType = accountType;
    }

    public long getAccountCreationDate() {
        return accountCreationDate;
    }

    public void setAccountCreationDate(long accountCreationDate) {
        this.accountCreationDate = accountCreationDate;
    }

    public boolean isAccountIsBlocked() {
        return accountIsBlocked;
    }

    public void setAccountIsBlocked(boolean accountIsBlocked) {
        this.accountIsBlocked = accountIsBlocked;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }
}
