package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyInvestCompany;
import ldbc.finbench.datagen.entities.edges.PersonInvestCompany;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
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
        Random chooseInvestorRand = randomFarm.get(RandomGeneratorFarm.Aspect.CHOOSE_COMPANY_INVESTOR);
        for (Company target : targets) {
            int numInvestors = numInvestorsRand.nextInt(
                DatagenParams.maxInvestors - DatagenParams.minInvestors + 1
            ) + DatagenParams.minInvestors;
            for (int i = 0; i < numInvestors; i++) {
                int index = chooseInvestorRand.nextInt(investors.size());
                Company investor = investors.get(index);
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
