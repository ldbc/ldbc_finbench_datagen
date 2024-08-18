package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonOwnAccount implements DynamicActivity, Serializable {
    private final Person person;
    private final Account account;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public PersonOwnAccount(Person person, Account account, long creationDate, long deletionDate,
                            boolean isExplicitlyDeleted, String comment) {
        this.person = person;
        this.account = account;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static void createPersonOwnAccount(RandomGeneratorFarm farm, Person person, Account account,
                                              long creationDate) {
        // Delete when account is deleted
        account.setOwnerType(PersonOrCompany.PERSON);
        account.setPersonOwner(person);
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComment(farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        PersonOwnAccount personOwnAccount = new PersonOwnAccount(person, account, creationDate,
                                                                 account.getDeletionDate(),
                                                                 account.isExplicitlyDeleted(),
                                                                 comment);
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

    public String getComment() {
        return comment;
    }
}
