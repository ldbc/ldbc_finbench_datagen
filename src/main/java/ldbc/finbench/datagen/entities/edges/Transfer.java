package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class Transfer implements DynamicActivity, Serializable {
    private Account fromAccount;
    private Account toAccount;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public Transfer(Account fromAccount, Account toAccount, long creationDate, long deletionDate,
                    boolean isExplicitlyDeleted) {
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Transfer createTransfer(Random random, Account fromAccount, Account toAccount) {
        long creationDate = Dictionaries.dates.randomAccountToAccountDate(random, fromAccount, toAccount);
        // TODO: calcu min delete date
        Transfer transfer = new Transfer(fromAccount, toAccount, creationDate, 0, false);
        fromAccount.getTransfers().add(transfer);

        return transfer;
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
