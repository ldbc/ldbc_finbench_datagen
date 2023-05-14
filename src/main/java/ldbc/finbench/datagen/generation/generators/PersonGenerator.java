package ldbc.finbench.datagen.generation.generators;

import java.util.Iterator;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonGenerator {
    private final RandomGeneratorFarm randomFarm;
    private int nextId = 0;

    public PersonGenerator() {
        this.randomFarm = new RandomGeneratorFarm();
    }

    private long composePersonId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 41);
        long bucket =
            (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
                .getSimulationEnd());
        return (bucket << 41) | ((id & idMask));
    }

    public Person generatePerson() {
        Person person = new Person();

        long creationDate =
            Dictionaries.dates.randomPersonCreationDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
        person.setCreationDate(creationDate);

        long personId = composePersonId(nextId++, creationDate);
        person.setPersonId(personId);

        String personname =
            Dictionaries.personNames.getUniformDistRandName(randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_NAME));
        person.setPersonName(personname);

        // Set blocked to false by default
        person.setBlocked(false);

        return person;
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
    }

    public Iterator<Person> generatePersonBlock(int blockId, int blockSize) {
        resetState(blockId);
        nextId = blockId * blockSize;
        return new Iterator<Person>() {
            private int personNum = 0;

            @Override
            public boolean hasNext() {
                return personNum < blockSize;
            }

            @Override
            public Person next() {
                ++personNum;
                return generatePerson();
            }
        };
    }

}
