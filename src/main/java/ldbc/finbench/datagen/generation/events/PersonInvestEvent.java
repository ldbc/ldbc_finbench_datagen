package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonInvestCompany;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonInvestEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public PersonInvestEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
    }

    public void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public void personInvest(Person person, Company target) {
        PersonInvestCompany.createPersonInvestCompany(randomFarm, person, target);
    }

    public void personInvestPartition(List<Person> investors, List<Company> targets) {
        Random numInvestorsRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUMS_PERSON_INVEST);
        Random chooseInvestorRand = randomFarm.get(RandomGeneratorFarm.Aspect.CHOOSE_PERSON_INVESTOR);
        for (Company target : targets) {
            int numInvestors = numInvestorsRand.nextInt(
                DatagenParams.maxInvestors - DatagenParams.minInvestors + 1
            ) + DatagenParams.minInvestors;
            for (int i = 0; i < numInvestors; i++) {
                int index = chooseInvestorRand.nextInt(investors.size());
                Person investor = investors.get(index);
                if (cannotInvest(investor, target)) {
                    continue;
                }
                PersonInvestCompany.createPersonInvestCompany(randomFarm, investor, target);
            }
            System.out.println("[personInvest]: person invest company " + numInvestors + " times");
        }
    }

    public boolean cannotInvest(Person investor, Company target) {
        return target.hasInvestedBy(investor);
    }
}
