package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonGuaranteePerson;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonGuaranteeEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public PersonGuaranteeEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public List<Person> personGuarantee(List<Person> persons, int blockId) {
        resetState(blockId);

        Random pickPersonRand = randomFarm.get(RandomGeneratorFarm.Aspect.PICK_PERSON_GUARANTEE);
        Random numGuaranteesRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_GUARANTEES_PER_PERSON);
        int numPersonsToTake = (int) (persons.size() * DatagenParams.personGuaranteeFraction);

        for (int i = 0; i < numPersonsToTake; i++) {
            Person from = persons.get(pickPersonRand.nextInt(persons.size()));
            int numGuarantees = numGuaranteesRand.nextInt(DatagenParams.maxTargetsToGuarantee);
            for (int j = 0; j < Math.max(1, numGuarantees); j++) {
                Person to = persons.get(randIndex.nextInt(persons.size()));
                if (from.canGuarantee(to)) {
                    PersonGuaranteePerson.createPersonGuaranteePerson(randomFarm, from, to);
                }
            }
        }

        return persons;
    }
}
