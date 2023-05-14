package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
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

    public List<PersonGuaranteePerson> personGuarantee(List<Person> persons, int blockId) {
        resetState(blockId);
        List<PersonGuaranteePerson> personGuaranteePeople = new ArrayList<>();
        for (int i = 0; i < persons.size(); i++) {
            Person person = persons.get(i);
            int targetsToGuarantee = targetsToGuaranteeRandom.nextInt(DatagenParams.maxTargetsToGuarantee);
            for (int j = 0; j < targetsToGuarantee; j++) {
                // Choose a random person to guarantee
                Person toPerson = persons.get(randIndex.nextInt(persons.size()));
                if (person.canGuarantee(toPerson)) {
                    PersonGuaranteePerson personGuaranteePerson = PersonGuaranteePerson.createPersonGuaranteePerson(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE), person, toPerson);
                    personGuaranteePeople.add(personGuaranteePerson);
                }
            }
        }
        return personGuaranteePeople;
    }
}
