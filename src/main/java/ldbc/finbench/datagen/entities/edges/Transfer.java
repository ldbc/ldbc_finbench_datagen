package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class Transfer implements DynamicActivity, Serializable {
    private long fromAccountId;
    private long toAccountId;
    private String toAccountType;
    private long toAccountCreationDate;
    private boolean toAccountIsBlocked;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public Transfer(long fromAccountId, long toAccountId, String toAccountType, long toAccountCreationDate,
                    boolean toAccountIsBlocked, long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.fromAccountId = fromAccountId;
        this.toAccountId = toAccountId;
        this.toAccountType = toAccountType;
        this.toAccountCreationDate = toAccountCreationDate;
        this.toAccountIsBlocked = toAccountIsBlocked;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Transfer createTransfer(Random random, Account fromAccount, Account toAccount) {
        long creationDate = Dictionaries.dates.randomAccountToAccountDate(random, fromAccount, toAccount);

        Transfer transfer = new Transfer(fromAccount.getAccountId(), toAccount.getAccountId(), toAccount.getType(),
                toAccount.getCreationDate(), toAccount.isBlocked(), creationDate, 0, false);
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

    public String getToAccountType() {
        return toAccountType;
    }

    public void setToAccountType(String toAccountType) {
        this.toAccountType = toAccountType;
    }

    public long getToAccountCreationDate() {
        return toAccountCreationDate;
    }

    public void setToAccountCreationDate(long toAccountCreationDate) {
        this.toAccountCreationDate = toAccountCreationDate;
    }

    public boolean isToAccountIsBlocked() {
        return toAccountIsBlocked;
    }

    public void setToAccountIsBlocked(boolean toAccountIsBlocked) {
        this.toAccountIsBlocked = toAccountIsBlocked;
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
