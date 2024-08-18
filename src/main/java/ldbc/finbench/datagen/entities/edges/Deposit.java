package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class Deposit implements DynamicActivity, Serializable {
    private final Loan loan;
    private final Account account;
    private final double amount;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;

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

    public Loan getLoan() {
        return loan;
    }

    public Account getAccount() {
        return account;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }
}
