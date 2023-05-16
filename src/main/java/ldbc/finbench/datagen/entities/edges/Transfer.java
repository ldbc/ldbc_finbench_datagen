package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class Transfer implements DynamicActivity, Serializable {
    private static final Map<String, AtomicLong> multiplicityMap = new ConcurrentHashMap<>();
    private Account fromAccount;
    private Account toAccount;
    private double amount;
    private long creationDate;
    private long deletionDate;
    private long multiplicityId;
    private boolean isExplicitlyDeleted;

    public Transfer(Account fromAccount, Account toAccount, double amount, long creationDate, long deletionDate,
                    long multiplicityId,
                    boolean isExplicitlyDeleted) {
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.amount = amount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.multiplicityId = multiplicityId;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Transfer createTransfer(Random random, Account from, Account to, double amount) {
        long deleteDate = Math.min(from.getDeletionDate(), to.getDeletionDate());
        long creationDate = Dictionaries.dates.randomAccountToAccountDate(random, from, to, deleteDate);
        boolean willDelete = from.isExplicitlyDeleted() && to.isExplicitlyDeleted();
        long multiplicityId = getMultiplicityIdAndInc(from, to);
        Transfer transfer = new Transfer(from, to, amount, creationDate, deleteDate, multiplicityId, willDelete);
        from.getTransferOuts().add(transfer);
        to.getTransferIns().add(transfer);

        return transfer;
    }

    public static long getMultiplicityIdAndInc(Account from, Account to) {
        String key = from.getAccountId() + "-" + to.getAccountId();
        AtomicLong atomicInt = multiplicityMap.computeIfAbsent(key, k -> new AtomicLong());
        return atomicInt.getAndIncrement();
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
