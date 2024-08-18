package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class Withdraw implements DynamicActivity, Serializable {
    private final Account fromAccount;
    private final Account toAccount;
    private final double amount;
    private final long creationDate;
    private final long deletionDate;
    private final long multiplicityId;
    private final boolean isExplicitlyDeleted;

    public Withdraw(Account fromAccount, Account toAccount, double amount, long creationDate, long deletionDate,
                    long multiplicityId, boolean isExplicitlyDeleted) {
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.amount = amount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.multiplicityId = multiplicityId;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Withdraw createWithdraw(Random random, Account from, Account to, long multiplicityId, double amount) {
        long deleteDate = Math.min(from.getDeletionDate(), to.getDeletionDate());
        long creationDate = Dictionaries.dates.randomAccountToAccountDate(random, from, to, deleteDate);
        boolean willDelete = from.isExplicitlyDeleted() && to.isExplicitlyDeleted();
        Withdraw withdraw = new Withdraw(from, to, amount, creationDate, deleteDate, multiplicityId, willDelete);
        from.getWithdraws().add(withdraw);

        return withdraw;
    }

    public double getAmount() {
        return amount;
    }

    public Account getFromAccount() {
        return fromAccount;
    }

    public Account getToAccount() {
        return toAccount;
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

    public long getMultiplicityId() {
        return multiplicityId;
    }
}
