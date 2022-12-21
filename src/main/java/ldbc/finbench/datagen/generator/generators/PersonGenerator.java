package ldbc.finbench.datagen.generator.generators;

import java.util.Iterator;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;
import ldbc.finbench.datagen.generator.dictionary.PersonNameDictionary;
import ldbc.finbench.datagen.generator.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonGenerator {

    private DegreeDistribution degreeDistribution;
    private PersonNameDictionary personNameDictionary;
    private RandomGeneratorFarm randomFarm;
    private int nextId = 0;

    public PersonGenerator(GeneratorConfiguration conf, String degreeDistribution){
        this.randomFarm = new RandomGeneratorFarm();
        this.degreeDistribution.initialize(conf);
        this.personNameDictionary = new PersonNameDictionary();
    }

    private long composePersonId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 41);
        long bucket =
                (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
                .getSimulationEnd());
        return (bucket << 41) | ((id & idMask));
    }

    private Person generatePerson() {

        long creationDate = Dictionaries.dates.randomPersonCreationDate(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
        long personId = composePersonId(nextId++, creationDate);
        String personSurname = Dictionaries.personNames.getGeoDistRandomName(
                randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_NAME),
                personNameDictionary.getNumNames());
        byte gender = (randomFarm.get(RandomGeneratorFarm.Aspect.GENDER)).nextDouble() > 0.5 ?
                (byte) 1 : (byte) 0;
        long maxDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.maxNumDegree);
        boolean isBlocked = false;

        return new Person(personId, personSurname, gender, creationDate, maxDegree, isBlocked);
    }

    public Iterator<Person> generatePersonBlock(int blockId, int blockSize) {
        nextId = blockId * blockSize;
        return new Iterator<Person>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < blockSize;
            }

            @Override
            public Person next() {
                ++i;
                return generatePerson();
            }
        };
    }

}
