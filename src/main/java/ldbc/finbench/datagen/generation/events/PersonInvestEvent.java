package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
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

    public PersonInvestCompany personInvest(Person person, Company company) {
        return PersonInvestCompany.createPersonInvestCompany(
            randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_INVEST_DATE),
            randomFarm.get(RandomGeneratorFarm.Aspect.INVEST_RATIO),
            person, company);
    }
}
