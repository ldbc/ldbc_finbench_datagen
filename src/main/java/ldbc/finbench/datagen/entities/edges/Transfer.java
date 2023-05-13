package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class Transfer implements DynamicActivity, Serializable {
    private Account fromAccount;
    private Account toAccount;
    private double amount;
    private long creationDate;
    private long deletionDate;
    private long multiplicityId;
    private boolean isExplicitlyDeleted;

    public Transfer(Account fromAccount, Account toAccount, long creationDate, long deletionDate, long multiplicityId,
                    boolean isExplicitlyDeleted) {
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.multiplicityId = multiplicityId;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Transfer createTransfer(Random random, Account fromAccount, Account toAccount, long multiplicityId) {
        long creationDate = Dictionaries.dates.randomAccountToAccountDate(random, fromAccount, toAccount);
        long deleteDate = Math.min(fromAccount.getDeletionDate(), toAccount.getDeletionDate());
        boolean willDelete = fromAccount.isExplicitlyDeleted() && toAccount.isExplicitlyDeleted();
        Transfer transfer = new Transfer(fromAccount, toAccount, creationDate, deleteDate, multiplicityId, willDelete);
        fromAccount.getTransferOuts().add(transfer);
        toAccount.getTransferIns().add(transfer);

        return transfer;
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

    public long getMultiplicityId() {
        return multiplicityId;
    }

    public void setMultiplicityId(long multiplicityId) {
        this.multiplicityId = multiplicityId;
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
