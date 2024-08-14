package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonGuaranteePerson;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonGuaranteeEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;
    private final Random targetsToGuaranteeRandom;

    public PersonGuaranteeEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
        targetsToGuaranteeRandom = new Random(DatagenParams.defaultSeed);
    }


    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
        targetsToGuaranteeRandom.setSeed(seed);
    }

    public List<Person> personGuarantee(List<Person> persons, int blockId) {
        resetState(blockId);

        Collections.shuffle(persons, randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_GURANTEE_SHUFFLE));
        int numPersonsToTake = (int)(persons.size() * DatagenParams.personGuaranteeFraction);
        for (int i = 0; i < numPersonsToTake; i++) {
            Person from = persons.get(i);
            int targetsToGuarantee = targetsToGuaranteeRandom.nextInt(DatagenParams.maxTargetsToGuarantee);
            for (int j = 0; j < targetsToGuarantee; j++) {
                Person to = persons.get(randIndex.nextInt(persons.size())); // Choose a random person
                if (from.canGuarantee(to)) {
                    PersonGuaranteePerson.createPersonGuaranteePerson(randomFarm, from, to);
                }
            }
        }

        return persons;
    }
}
