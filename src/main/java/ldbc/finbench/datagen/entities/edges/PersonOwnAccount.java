package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class PersonOwnAccount implements DynamicActivity, Serializable {
    private Person person;
    private Account account;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public PersonOwnAccount(Person person, Account account, long creationDate, long deletionDate,
                            boolean isExplicitlyDeleted) {
        this.person = person;
        this.account = account;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static PersonOwnAccount createPersonOwnAccount(Random random, Person person, Account account) {
        long creationDate = Dictionaries.dates.randomPersonToAccountDate(random, person, account);
        // Delete when account is deleted
        PersonOwnAccount personOwnAccount = new PersonOwnAccount(person, account, creationDate,
                                                                 account.getDeletionDate(),
                                                                 account.isExplicitlyDeleted());
        person.getPersonOwnAccounts().add(personOwnAccount);
        return personOwnAccount;
    }

    public Person getPerson() {
        return person;
    }

    public void setPersonId(Person person) {
        this.person = person;
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
