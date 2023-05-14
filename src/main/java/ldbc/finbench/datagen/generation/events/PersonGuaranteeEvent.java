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

    public PersonGuaranteeEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
    }


    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public List<PersonGuaranteePerson> personGuarantee(List<Person> persons, int blockId) {
        resetState(blockId);
        List<PersonGuaranteePerson> personGuaranteePeople = new ArrayList<>();
        // TODO
        for (int i = 0; i < persons.size(); i++) {
            Person p = persons.get(i);
            int personIndex = randIndex.nextInt(persons.size());
            PersonGuaranteePerson personGuaranteePerson = PersonGuaranteePerson.createPersonGuaranteePerson(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                p,
                persons.get(personIndex));
            personGuaranteePeople.add(personGuaranteePerson);
        }
        return personGuaranteePeople;
    }
}
