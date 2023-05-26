package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyInvestCompany;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyInvestEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public CompanyInvestEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
    }

    public void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public CompanyInvestCompany companyInvest(Company investor, Company invested) {
        return CompanyInvestCompany.createCompanyInvestCompany(
            randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_INVEST_DATE),
            randomFarm.get(RandomGeneratorFarm.Aspect.INVEST_RATIO),
            investor, invested);
    }
}
