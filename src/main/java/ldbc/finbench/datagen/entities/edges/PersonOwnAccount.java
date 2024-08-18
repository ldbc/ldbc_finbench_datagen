package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;

public class PersonOwnAccount implements DynamicActivity, Serializable {
    private final Person person;
    private final Account account;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;

    public PersonOwnAccount(Person person, Account account, long creationDate, long deletionDate,
                            boolean isExplicitlyDeleted) {
        this.person = person;
        this.account = account;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static void createPersonOwnAccount(Person person, Account account, long creationDate) {
        // Delete when account is deleted
        account.setOwnerType(PersonOrCompany.PERSON);
        account.setPersonOwner(person);
        PersonOwnAccount personOwnAccount = new PersonOwnAccount(person, account, creationDate,
                                                                 account.getDeletionDate(),
                                                                 account.isExplicitlyDeleted());
        person.getPersonOwnAccounts().add(personOwnAccount);
    }

    public Person getPerson() {
        return person;
    }

    public Account getAccount() {
        return account;
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
}
