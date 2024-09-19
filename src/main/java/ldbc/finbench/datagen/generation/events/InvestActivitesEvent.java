package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyInvestCompany;
import ldbc.finbench.datagen.entities.edges.PersonInvestCompany;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class InvestActivitesEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;

    public InvestActivitesEvent() {
        randomFarm = new RandomGeneratorFarm();
    }

    public void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
    }

    public List<Company> investPartition(List<Person> personinvestors, List<Company> companyInvestors,
                                         List<Company> targets) {
        Random numPersonInvestorsRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUMS_PERSON_INVEST);
        Random choosePersonInvestorRand = randomFarm.get(RandomGeneratorFarm.Aspect.CHOOSE_PERSON_INVESTOR);
        Random numCompanyInvestorsRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUMS_COMPANY_INVEST);
        Random chooseCompanyInvestorRand = randomFarm.get(RandomGeneratorFarm.Aspect.CHOOSE_COMPANY_INVESTOR);
        for (Company target : targets) {
            // Person investors
            int numPersonInvestors = numPersonInvestorsRand.nextInt(
                DatagenParams.maxInvestors - DatagenParams.minInvestors + 1
            ) + DatagenParams.minInvestors;
            for (int i = 0; i < numPersonInvestors; i++) {
                int index = choosePersonInvestorRand.nextInt(personinvestors.size());
                Person investor = personinvestors.get(index);
                if (cannotInvest(investor, target)) {
                    continue;
                }
                PersonInvestCompany.createPersonInvestCompany(randomFarm, investor, target);
            }

            // Company investors
            int numCompanyInvestors = numCompanyInvestorsRand.nextInt(
                DatagenParams.maxInvestors - DatagenParams.minInvestors + 1
            ) + DatagenParams.minInvestors;
            for (int i = 0; i < numCompanyInvestors; i++) {
                int index = chooseCompanyInvestorRand.nextInt(companyInvestors.size());
                Company investor = companyInvestors.get(index);
                if (cannotInvest(investor, target)) {
                    continue;
                }
                CompanyInvestCompany.createCompanyInvestCompany(randomFarm, investor, target);
            }
        }
        return targets;
    }

    public boolean cannotInvest(Person investor, Company target) {
        return target.hasInvestedBy(investor);
    }

    public boolean cannotInvest(Company investor, Company target) {
        return (investor == target) || investor.hasInvestedBy(target) || target.hasInvestedBy(investor);
    }
}
