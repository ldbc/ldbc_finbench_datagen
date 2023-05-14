package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class Withdraw implements DynamicActivity, Serializable {
    private Account fromAccount;
    private Account toAccount;
    private double amount;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public Withdraw(Account fromAccount, Account toAccount,
                    long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Withdraw createWithdraw(Random random, Account from, Account to) {
        long deleteDate = Math.min(from.getDeletionDate(), to.getDeletionDate());
        long creationDate = Dictionaries.dates.randomAccountToAccountDate(random, from, to, deleteDate);
        boolean willDelete = from.isExplicitlyDeleted() && to.isExplicitlyDeleted();
        Withdraw withdraw = new Withdraw(from, to, creationDate, deleteDate, willDelete);
        from.getWithdraws().add(withdraw);

        return withdraw;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public Account getFromAccount() {
        return fromAccount;
    }

    public void setFromAccount(Account fromAccount) {
        this.fromAccount = fromAccount;
    }

    public Account getToAccount() {
        return toAccount;
    }

    public void setToAccount(Account toAccount) {
        this.toAccount = toAccount;
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
