package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonGuaranteePerson;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonGuaranteeEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;
    private final Random targetsToGuaranteeRandom;
    private final double probGuarantee;

    public PersonGuaranteeEvent(double probGuarantee) {
        this.probGuarantee = probGuarantee;
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

        persons.forEach(person -> {
            if (randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_WHETHER_GURANTEE).nextDouble() < probGuarantee) {
                int targetsToGuarantee = targetsToGuaranteeRandom.nextInt(DatagenParams.maxTargetsToGuarantee);
                for (int j = 0; j < targetsToGuarantee; j++) {
                    Person toPerson = persons.get(randIndex.nextInt(persons.size())); // Choose a random person
                    if (person.canGuarantee(toPerson)) {
                        PersonGuaranteePerson.createPersonGuaranteePerson(randomFarm, person, toPerson);
                    }
                }
            }
        });

        return persons;
    }
}
