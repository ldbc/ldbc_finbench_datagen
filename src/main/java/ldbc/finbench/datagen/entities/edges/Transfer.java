package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class Transfer implements DynamicActivity, Serializable {
    private long fromAccountId;
    private long toAccountId;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public Transfer(long fromAccountId, long toAccountId,
                    long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.fromAccountId = fromAccountId;
        this.toAccountId = toAccountId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Transfer createTransfer(Random random, Account fromAccount, Account toAccount) {
        long creationDate = Dictionaries.dates.randomAccountToAccountDate(random, fromAccount, toAccount);

        Transfer transfer = new Transfer(fromAccount.getAccountId(), toAccount.getAccountId(),
                creationDate, 0, false);
        fromAccount.getTransfers().add(transfer);

        return transfer;
    }

    public long getFromAccountId() {
        return fromAccountId;
    }

    public void setFromAccountId(long fromAccountId) {
        this.fromAccountId = fromAccountId;
    }

    public long getToAccountId() {
        return toAccountId;
    }

    public void setToAccountId(long toAccountId) {
        this.toAccountId = toAccountId;
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
