package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.WorkIn;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class WorkInEvent implements Serializable {
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public WorkInEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }

    public List<WorkIn> workIn(List<Person> persons, List<Company> companies, int blockId) {
        random.setSeed(blockId);
        List<WorkIn> workIns = new ArrayList<>();

        for (int i = 0; i < persons.size(); i++) {
            Person p = persons.get(i);
            int companyIndex = randIndex.nextInt(companies.size());

            if (work()) {
                WorkIn workIn = WorkIn.createWorkIn(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        p,
                        companies.get(companyIndex));
                workIns.add(workIn);
            }
        }
        return workIns;
    }

    private boolean work() {
        //TODO determine whether to generate workIn
        return true;
    }
}
