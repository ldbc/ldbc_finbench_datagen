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
    private final Random targetsToGuaranteeRandom;

    public CompanyGuaranteeEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
        targetsToGuaranteeRandom = new Random(DatagenParams.defaultSeed);
    }


    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
        targetsToGuaranteeRandom.setSeed(seed);
    }

    public List<CompanyGuaranteeCompany> companyGuarantee(List<Company> companies, int blockId) {
        resetState(blockId);
        List<CompanyGuaranteeCompany> companyGuaranteeCompanies = new ArrayList<>();
        for (int i = 0; i < companies.size(); i++) {
            Company company = companies.get(i);
            int targetsToGuarantee = targetsToGuaranteeRandom.nextInt(DatagenParams.maxTargetsToGuarantee);
            for (int j = 0; j < targetsToGuarantee; j++) {
                // Choose a random company to guarantee
                Company toCompany = companies.get(randIndex.nextInt(companies.size()));
                if (company.canGuarantee(toCompany)) {
                    CompanyGuaranteeCompany companyGuaranteeCompany =
                        CompanyGuaranteeCompany.createCompanyGuaranteeCompany(
                            randomFarm.get(RandomGeneratorFarm.Aspect.DATE), company, toCompany);
                    companyGuaranteeCompanies.add(companyGuaranteeCompany);
                }
            }
        }
        return companyGuaranteeCompanies;
    }
}
