package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
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

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public List<CompanyInvestCompany> companyInvest(List<Company> companies, int blockId) {
        resetState(blockId);
        List<CompanyInvestCompany> companyInvestCompanies = new ArrayList<>();
        // TODO: person can invest multiple companies
        for (int i = 0; i < companies.size(); i++) {
            Company c = companies.get(i);
            // TODO: companyIndex can not equal to i
            int companyIndex = randIndex.nextInt(companies.size());
            CompanyInvestCompany companyInvestCompany = CompanyInvestCompany.createCompanyInvestCompany(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                c,
                companies.get(companyIndex));
            companyInvestCompanies.add(companyInvestCompany);

        }
        return companyInvestCompanies;
    }
}
