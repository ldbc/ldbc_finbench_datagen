package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class PersonOwnAccount implements DynamicActivity, Serializable {
    private long personId;
    private Account account;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public PersonOwnAccount(long personId, Account account,
                            long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.personId = personId;
        this.account = account;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static PersonOwnAccount createPersonOwnAccount(Random random, Person person, Account account) {
        long creationDate = Dictionaries.dates.randomPersonToAccountDate(random, person, account);

        PersonOwnAccount personOwnAccount = new PersonOwnAccount(person.getPersonId(), account,
                creationDate, 0, false);
        person.getPersonOwnAccounts().add(personOwnAccount);
        return personOwnAccount;
    }

    public long getPersonId() {
        return personId;
    }

    public void setPersonId(long personId) {
        this.personId = personId;
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

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }
}
