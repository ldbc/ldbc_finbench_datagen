package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class Repay implements DynamicActivity, Serializable {
    private Account account;
    private Loan loan;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public Repay(Account account, Loan loan, long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.account = account;
        this.loan = loan;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Repay createRepay(Random random, Account account, Loan loan) {
        long creationDate = Dictionaries.dates.randomAccountToLoanDate(random, account, loan);
        Repay repay = new Repay(account, loan, creationDate, 0, false);
        account.getRepays().add(repay);

        return repay;
    }

    public Account getAccount() {
        return account;
    }

    public void setAccount(Account account) {
        this.account = account;
    }

    public Loan getLoan() {
        return loan;
    }

    public void setLoan(Loan loan) {
        this.loan = loan;
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
