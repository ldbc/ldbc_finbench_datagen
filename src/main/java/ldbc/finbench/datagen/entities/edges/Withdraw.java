package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class Withdraw implements DynamicActivity, Serializable {
    private final long fromAccountId;
    private final long toAccountId;
    private final String fromAccountType;
    private final String toAccountType;
    private final double amount;
    private final long creationDate;
    private final long deletionDate;
    private final long multiplicityId;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public Withdraw(Account fromAccount, Account toAccount, double amount, long creationDate, long deletionDate,
                    long multiplicityId, boolean isExplicitlyDeleted, String comment) {
        this.fromAccountId = fromAccount.getAccountId();
        this.toAccountId = toAccount.getAccountId();
        this.fromAccountType = fromAccount.getType();
        this.toAccountType = toAccount.getType();
        this.amount = amount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.multiplicityId = multiplicityId;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static void createWithdraw(RandomGeneratorFarm farm, Account from, Account to, long multiplicityId) {
        Random dateRand = farm.get(RandomGeneratorFarm.Aspect.WITHDRAW_DATE);
        long deleteDate = Math.min(from.getDeletionDate(), to.getDeletionDate());
        long creationDate = Dictionaries.dates.randomAccountToAccountDate(dateRand, from, to, deleteDate);
        boolean willDelete = from.isExplicitlyDeleted() && to.isExplicitlyDeleted();
        double amount =
            farm.get(RandomGeneratorFarm.Aspect.WITHDRAW_AMOUNT).nextDouble() * DatagenParams.withdrawMaxAmount;
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        Withdraw withdraw =
            new Withdraw(from, to, amount, creationDate, deleteDate, multiplicityId, willDelete, comment);
        from.getWithdraws().add(withdraw);
    }

    public double getAmount() {
        return amount;
    }

    public long getFromAccountId() {
        return fromAccountId;
    }

    public long getToAccountId() {
        return toAccountId;
    }

    public String getFromAccountType() {
        return fromAccountType;
    }

    public String getToAccountType() {
        return toAccountType;
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

    public String getComment() {
        return comment;
    }
}
