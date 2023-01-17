package ldbc.finbench.datagen.generator.events;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyInvestCompany;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyInvestEvent {
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public CompanyInvestEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }

    public List<CompanyInvestCompany> companyInvest(List<Company> companies, int blockId) {
        random.setSeed(blockId);
        List<CompanyInvestCompany> companyInvestCompanies = new ArrayList<>();

        for (int i = 0; i < companies.size(); i++) {
            Company c = companies.get(i);
            int companyIndex = randIndex.nextInt(companies.size());

            if (invest()) {
                CompanyInvestCompany companyInvestCompany = CompanyInvestCompany.createCompanyInvestCompany(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        c,
                        companies.get(companyIndex));
                companyInvestCompanies.add(companyInvestCompany);
            }
        }
        return companyInvestCompanies;
    }

    private boolean invest() {
        //TODO determine whether to generate Invest
        return true;
    }
}