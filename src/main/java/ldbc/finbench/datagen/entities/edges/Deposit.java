package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class Deposit implements DynamicActivity, Serializable {
    private long loanId;
    private long accountId;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public Deposit(long loanId, long accountId, long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.loanId = loanId;
        this.accountId = accountId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static void createDeposit(Random random, Loan loan, Account account) {
        long creationDate = Dictionaries.dates.randomLoanToAccountDate(random, loan, account);

        loan.getDeposits().add(new Deposit(loan.getLoanId(), account.getAccountId(),
                creationDate, 0, false));
    }

    public long getLoanId() {
        return loanId;
    }

    public void setLoanId(long loanId) {
        this.loanId = loanId;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
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
