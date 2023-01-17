package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class Repay implements DynamicActivity, Serializable {
    private long accountId;
    private long loanId;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public Repay(long accountId, long loanId, long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.accountId = accountId;
        this.loanId = loanId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Repay createRepay(Random random, Account account, Loan loan) {
        long creationDate = Dictionaries.dates.randomAccountToLoanDate(random, account, loan);

        Repay repay = new Repay(account.getAccountId(), loan.getLoanId(),
                creationDate, 0, false);
        account.getRepays().add(repay);

        return repay;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    public long getLoanId() {
        return loanId;
    }

    public void setLoanId(long loanId) {
        this.loanId = loanId;
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
