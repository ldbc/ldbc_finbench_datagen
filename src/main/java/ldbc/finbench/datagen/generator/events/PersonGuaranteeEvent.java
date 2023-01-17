package ldbc.finbench.datagen.generator.events;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonGuaranteePerson;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonGuaranteeEvent {
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public PersonGuaranteeEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }

    public List<PersonGuaranteePerson> personGuarantee(List<Person> persons, int blockId) {
        random.setSeed(blockId);
        List<PersonGuaranteePerson> personGuaranteePeople = new ArrayList<>();

        for (int i = 0; i < persons.size(); i++) {
            Person p = persons.get(i);
            int personIndex = randIndex.nextInt(persons.size());

            if (guarantee()) {
                PersonGuaranteePerson personGuaranteePerson = PersonGuaranteePerson.createPersonGuaranteePerson(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        p,
                        persons.get(personIndex));
                personGuaranteePeople.add(personGuaranteePerson);
            }
        }
        return personGuaranteePeople;
    }

    private boolean guarantee() {
        //TODO determine whether to generate guarantee
        return true;
    }
}
