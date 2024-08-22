package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
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

    public void companyInvest(Company investor, Company target) {
        CompanyInvestCompany.createCompanyInvestCompany(randomFarm, investor, target);
    }

    public void companyInvestPartition(List<Company> investors, List<Company> targets) {
        Random numInvestorsRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUMS_COMPANY_INVEST);
        Collections.shuffle(investors, numInvestorsRand);
        for (Company target : targets) {
            int numInvestors = numInvestorsRand.nextInt(
                DatagenParams.maxInvestors - DatagenParams.minInvestors + 1
            ) + DatagenParams.minInvestors;
            int offset = numInvestorsRand.nextInt(investors.size() - numInvestors + 1);
            for (int i = 0; i < numInvestors; i++) {
                Company investor = investors.get(offset + i);
                if (cannotInvest(investor, target)) {
                    continue;
                }
                CompanyInvestCompany.createCompanyInvestCompany(randomFarm, investor, target);
            }
        }
    }

    public boolean cannotInvest(Company investor, Company target) {
        return (investor == target) || investor.hasInvestedBy(target) || target.hasInvestedBy(investor);
    }
}
