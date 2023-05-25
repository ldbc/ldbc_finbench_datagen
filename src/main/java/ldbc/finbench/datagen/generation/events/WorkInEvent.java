package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.WorkIn;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class WorkInEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public WorkInEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public List<WorkIn> workIn(List<Person> persons, List<Company> companies, int blockId) {
        resetState(blockId);
        List<WorkIn> workIns = new ArrayList<>();

        for (Person p : persons) {
            int companyIndex = randIndex.nextInt(companies.size());
            WorkIn workIn = WorkIn.createWorkIn(
                randomFarm.get(RandomGeneratorFarm.Aspect.WORKIN_DATE),
                p,
                companies.get(companyIndex));
            workIns.add(workIn);
        }
        return workIns;
    }
}
