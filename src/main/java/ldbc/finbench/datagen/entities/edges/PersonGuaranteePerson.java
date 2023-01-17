package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class PersonGuaranteePerson implements DynamicActivity, Serializable {
    private long fromPersonId;
    private long toPersonId;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public PersonGuaranteePerson(long fromPersonId, long toPersonId,
                                 long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.fromPersonId = fromPersonId;
        this.toPersonId = toPersonId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static PersonGuaranteePerson createPersonGuaranteePerson(Random random, Person fromPerson, Person toPerson) {
        long creationDate = Dictionaries.dates.randomPersonToPersonDate(random, fromPerson, toPerson);

        PersonGuaranteePerson personGuaranteePerson = new PersonGuaranteePerson(fromPerson.getPersonId(),
                toPerson.getPersonId(), creationDate, 0, false);
        fromPerson.getPersonGuaranteePeople().add(personGuaranteePerson);

        return personGuaranteePerson;
    }

    public long getFromPersonId() {
        return fromPersonId;
    }

    public void setFromPersonId(long fromPersonId) {
        this.fromPersonId = fromPersonId;
    }

    public long getToPersonId() {
        return toPersonId;
    }

    public void setToPersonId(long toPersonId) {
        this.toPersonId = toPersonId;
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
