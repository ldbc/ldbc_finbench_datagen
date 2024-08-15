package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
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

    public List<Company> companyGuarantee(List<Company> companies, int blockId) {
        resetState(blockId);

        Random pickCompanyRand = randomFarm.get(RandomGeneratorFarm.Aspect.PICK_COMPANY_GUARANTEE);
        Random numGuaranteesRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_GUARANTEES_PER_COMPANY);
        Random dateRand = randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_GUARANTEE_DATE);
        int numCompaniesToTake = (int) (companies.size() * DatagenParams.companyGuaranteeFraction);

        for (int i = 0; i < numCompaniesToTake; i++) {
            Company from = companies.get(pickCompanyRand.nextInt(companies.size()));
            int numGuarantees = numGuaranteesRand.nextInt(DatagenParams.maxTargetsToGuarantee);
            for (int j = 0; j < Math.max(1,numGuarantees); j++) {
                Company to = companies.get(randIndex.nextInt(companies.size()));
                if (from.canGuarantee(to)) {
                    CompanyGuaranteeCompany.createCompanyGuaranteeCompany(dateRand, from, to);
                }
            }
        }

        return companies;
    }
}
