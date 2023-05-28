package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Comparator;
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

    public Transfer(Account fromAccount, Account toAccount, double amount, long creationDate, long deletionDate,
                    long multiplicityId, boolean isExplicitlyDeleted) {
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.amount = amount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.multiplicityId = multiplicityId;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    // Note: used in account centric implementation
    public static void createTransfer(Random random, Account from, Account to, long multiplicityId, double amount) {
        long deleteDate = Math.min(from.getDeletionDate(), to.getDeletionDate());
        long creationDate = Dictionaries.dates.randomAccountToAccountDate(random, from, to, deleteDate);
        boolean willDelete = from.isExplicitlyDeleted() && to.isExplicitlyDeleted();
        Transfer transfer = new Transfer(from, to, amount, creationDate, deleteDate, multiplicityId, willDelete);
        from.getTransferOuts().add(transfer);
        to.getTransferIns().add(transfer);
    }

    public static Transfer createTransferAndReturn(Random random, Account from, Account to, long multiplicityId,
                                                   double amount) {
        long deleteDate = Math.min(from.getDeletionDate(), to.getDeletionDate());
        long creationDate = Dictionaries.dates.randomAccountToAccountDate(random, from, to, deleteDate);
        boolean willDelete = from.isExplicitlyDeleted() && to.isExplicitlyDeleted();
        Transfer transfer = new Transfer(from, to, amount, creationDate, deleteDate, multiplicityId, willDelete);
        from.getTransferOuts().add(transfer);
        to.getTransferIns().add(transfer);
        return transfer;
    }

    public static class FullComparator implements Comparator<Transfer> {

        public int compare(Transfer a, Transfer b) {
            long res = (a.fromAccount.getAccountId() - b.fromAccount.getAccountId());
            if (res > 0) {
                return 1;
            }
            if (res < 0) {
                return -1;
            }
            long res2 = a.creationDate - b.getCreationDate();
            if (res2 > 0) {
                return 1;
            }
            if (res2 < 0) {
                return -1;
            }
            return 0;
        }

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

    @Override
    public String toString() {
        return "Transfer{" + "fromAccount=" + fromAccount + ", toAccount=" + toAccount + ", amount=" + amount
            + ", creationDate=" + creationDate + ", deletionDate=" + deletionDate + ", multiplicityId=" + multiplicityId
            + ", isExplicitlyDeleted=" + isExplicitlyDeleted + '}';
    }
}
