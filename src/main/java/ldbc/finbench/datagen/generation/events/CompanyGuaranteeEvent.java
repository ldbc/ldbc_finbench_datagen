package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyGuaranteeCompany;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyGuaranteeEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public CompanyGuaranteeEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
    }


    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public List<CompanyGuaranteeCompany> companyGuarantee(List<Company> companies, int blockId) {
        resetState(blockId);
        List<CompanyGuaranteeCompany> companyGuaranteeCompanies = new ArrayList<>();

        // TODO
        for (int i = 0; i < companies.size(); i++) {
            Company c = companies.get(i);
            int companyIndex = randIndex.nextInt(companies.size());
            CompanyGuaranteeCompany companyGuaranteeCompany = CompanyGuaranteeCompany.createCompanyGuaranteeCompany(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                c,
                companies.get(companyIndex));
            companyGuaranteeCompanies.add(companyGuaranteeCompany);
        }
        return companyGuaranteeCompanies;
    }
}
