package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class SignIn implements DynamicActivity, Serializable {
    private long mediumId;
    private long accountId;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public SignIn(long mediumId, long accountId, long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.mediumId = mediumId;
        this.accountId = accountId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static void createSignIn(Random random, Medium medium, Account account) {
        long creationDate = Dictionaries.dates.randomMediumToAccountDate(random, medium, account);

        medium.getSignIns().add(new SignIn(medium.getMediumId(), account.getAccountId(),
                creationDate, 0, false));
    }

    public long getMediumId() {
        return mediumId;
    }

    public void setMediumId(long mediumId) {
        this.mediumId = mediumId;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
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
