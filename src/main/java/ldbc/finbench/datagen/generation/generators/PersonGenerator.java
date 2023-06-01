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
        // Set creation date
        long creationDate =
            Dictionaries.dates.randomPersonCreationDate(randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_DATE));
        person.setCreationDate(creationDate);
        // Set person id
        long personId = composePersonId(nextId++, creationDate);
        person.setPersonId(personId);
        // Set person name
        String personname =
            Dictionaries.personNames.getUniformDistRandName(randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_NAME));
        person.setPersonName(personname);
        // Set blocked to false by default
        person.setBlocked(false);
        // Set gender TODO: add ranker related with person name
        person.setGender((randomFarm.get(RandomGeneratorFarm.Aspect.GENDER).nextDouble() > 0.5) ? (byte) 1 : (byte) 0);
        // Set birthday
        long birthday =
            Dictionaries.dates.randomPersonBirthday(randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_BIRTHDAY));
        person.setBirthday(birthday);
        // Set country and city
        int countryId =
            Dictionaries.places.getCountryForPerson(randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_COUNTRY));
        person.setCountryId(countryId);
        person.setCityId(
            Dictionaries.places.getRandomCity(randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_CITY), countryId));

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
