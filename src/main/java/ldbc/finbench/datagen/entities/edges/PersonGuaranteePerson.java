package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class PersonGuaranteePerson implements DynamicActivity, Serializable {
    private Person fromPerson;
    private Person toPerson;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public PersonGuaranteePerson(Person fromPerson, Person toPerson,
                                 long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.fromPerson = fromPerson;
        this.toPerson = toPerson;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static PersonGuaranteePerson createPersonGuaranteePerson(Random random, Person fromPerson, Person toPerson) {
        long creationDate = Dictionaries.dates.randomPersonToPersonDate(random, fromPerson, toPerson);
        PersonGuaranteePerson personGuaranteePerson = new PersonGuaranteePerson(fromPerson,
                                                                                toPerson, creationDate, 0, false);
        fromPerson.getPersonGuaranteePeople().add(personGuaranteePerson);

        return personGuaranteePerson;
    }

    public Person getFromPerson() {
        return fromPerson;
    }

    public void setFromPerson(Person fromPerson) {
        this.fromPerson = fromPerson;
    }

    public Person getToPerson() {
        return toPerson;
    }

    public void setToPerson(Person toPerson) {
        this.toPerson = toPerson;
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
