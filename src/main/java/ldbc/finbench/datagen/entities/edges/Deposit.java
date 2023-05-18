package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class Deposit implements DynamicActivity, Serializable {
    private Loan loan;
    private Account account;
    private double amount;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public Deposit(Loan loan, Account account, double amount, long creationDate, long deletionDate,
                   boolean isExplicitlyDeleted) {
        this.loan = loan;
        this.account = account;
        this.amount = amount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Deposit createDeposit(Random random, Loan loan, Account account, double amount) {
        long creationDate =
            Dictionaries.dates.randomLoanToAccountDate(random, loan, account, account.getDeletionDate());
        Deposit deposit =
            new Deposit(loan, account, amount, creationDate, account.getDeletionDate(), account.isExplicitlyDeleted());
        loan.addDeposit(deposit);
        account.getDeposits().add(deposit);

        return deposit;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public Loan getLoan() {
        return loan;
    }

    public void setLoan(Loan loan) {
        this.loan = loan;
    }

    public Account getAccount() {
        return account;
    }

    public void setAccount(Account account) {
        this.account = account;
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
