package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonGuaranteePerson implements DynamicActivity, Serializable {
    private final Person fromPerson;
    private final Person toPerson;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String relationship;

    public PersonGuaranteePerson(Person fromPerson, Person toPerson,
                                 long creationDate, long deletionDate, boolean isExplicitlyDeleted, String relation) {
        this.fromPerson = fromPerson;
        this.toPerson = toPerson;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.relationship = relation;
    }

    public static void createPersonGuaranteePerson(RandomGeneratorFarm randomFarm, Person fromPerson, Person toPerson) {
        long creationDate = Dictionaries.dates.randomPersonToPersonDate(
            randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_GUARANTEE_DATE), fromPerson, toPerson);
        String relation = Dictionaries.guaranteeRelationships.getDistributedText(
            randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_GUARANTEE_RELATIONSHIP));
        PersonGuaranteePerson personGuaranteePerson =
            new PersonGuaranteePerson(fromPerson, toPerson, creationDate, 0, false, relation);
        fromPerson.getGuaranteeSrc().add(personGuaranteePerson);
        toPerson.getGuaranteeDst().add(personGuaranteePerson);
    }

    public Person getFromPerson() {
        return fromPerson;
    }

    public Person getToPerson() {
        return toPerson;
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

    public String getRelationship() {
        return relationship;
    }
}
