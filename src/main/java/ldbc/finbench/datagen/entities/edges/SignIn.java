package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class SignIn implements DynamicActivity, Serializable {
    private Medium medium;
    private Account account;
    private long creationDate;
    private long deletionDate;
    private long multiplicityId;
    private boolean isExplicitlyDeleted;

    public SignIn(Medium medium, Account account, int mid, long creationDate, long deletionDate,
                  boolean isExplicitlyDeleted) {
        this.medium = medium;
        this.account = account;
        this.multiplicityId = mid;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static SignIn createSignIn(int mid, Random random, Medium medium, Account account) {
        long creationDate =
            Dictionaries.dates.randomMediumToAccountDate(random, medium, account, account.getDeletionDate());
        SignIn signIn = new SignIn(medium, account, mid, creationDate, account.getDeletionDate(),
                                   account.isExplicitlyDeleted());
        medium.getSignIns().add(signIn);
        account.getSignIns().add(signIn);

        return signIn;
    }

    public Medium getMedium() {
        return medium;
    }

    public void setMedium(Medium medium) {
        this.medium = medium;
    }

    public Account getAccount() {
        return account;
    }

    public void setAccount(Account account) {
        this.account = account;
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

    public long getMultiplicityId() {
        return multiplicityId;
    }

    public void setMultiplicityId(long multiplicityId) {
        this.multiplicityId = multiplicityId;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }
}
