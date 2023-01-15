package ldbc.finbench.datagen.generator.events;

import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.WorkIn;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class WorkInEvent {
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public WorkInEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }

    public void workIn(List<Person> persons, List<Company> companies, int blockId) {
        random.setSeed(blockId);

        for (int i = 0; i < persons.size(); i++) {
            Person p = persons.get(i);
            int companyIndex = randIndex.nextInt(companies.size());

            if (work()) {
                WorkIn.createWorkIn(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        p,
                        companies.get(companyIndex));
            }
        }
    }

    private boolean work() {
        //TODO determine whether to generate workIn
        return true;
    }
}
