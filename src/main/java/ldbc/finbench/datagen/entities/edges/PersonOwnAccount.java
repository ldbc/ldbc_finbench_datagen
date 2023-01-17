package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class PersonOwnAccount implements DynamicActivity, Serializable {
    private long personId;
    private long accountId;
    private String accountType;
    private long accountCreationDate;
    private boolean accountIsBlocked;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public PersonOwnAccount(long personId, long accountId, String accountType,
                            long accountCreationDate, boolean accountIsBlocked,
                            long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.personId = personId;
        this.accountId = accountId;
        this.accountType = accountType;
        this.accountCreationDate = accountCreationDate;
        this.accountIsBlocked = accountIsBlocked;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static PersonOwnAccount createPersonOwnAccount(Random random, Person person, Account account) {
        long creationDate = Dictionaries.dates.randomPersonToAccountDate(random, person, account);

        PersonOwnAccount personOwnAccount = new PersonOwnAccount(person.getPersonId(),
                account.getAccountId(), account.getType(), account.getCreationDate(),
                account.isBlocked(), creationDate, 0, false);
        person.getPersonOwnAccounts().add(personOwnAccount);
        return personOwnAccount;
    }

    public long getPersonId() {
        return personId;
    }

    public void setPersonId(long personId) {
        this.personId = personId;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    public String getAccountType() {
        return accountType;
    }

    public void setAccountType(String accountType) {
        this.accountType = accountType;
    }

    public long getAccountCreationDate() {
        return accountCreationDate;
    }

    public void setAccountCreationDate(long accountCreationDate) {
        this.accountCreationDate = accountCreationDate;
    }

    public boolean isAccountIsBlocked() {
        return accountIsBlocked;
    }

    public void setAccountIsBlocked(boolean accountIsBlocked) {
        this.accountIsBlocked = accountIsBlocked;
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
