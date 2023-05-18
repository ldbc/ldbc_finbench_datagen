package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class Repay implements DynamicActivity, Serializable {
    private Account account;
    private Loan loan;
    private double amount;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public Repay(Account account, Loan loan, double amount, long creationDate, long deletionDate,
                 boolean isExplicitlyDeleted) {
        this.account = account;
        this.loan = loan;
        this.amount = amount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Repay createRepay(Random random, Account account, Loan loan, double amount) {
        long creationDate =
            Dictionaries.dates.randomAccountToLoanDate(random, account, loan, account.getDeletionDate());
        Repay repay = new Repay(account, loan, amount, creationDate, account.getDeletionDate(),
                                account.isExplicitlyDeleted());
        loan.addRepay(repay);
        account.getRepays().add(repay);

        return repay;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
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
