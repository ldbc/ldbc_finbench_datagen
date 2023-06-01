package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyGuaranteeCompany;
import ldbc.finbench.datagen.entities.edges.PersonGuaranteePerson;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyGuaranteeEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;
    private final Random targetsToGuaranteeRandom;
    private final double probGuarantee;

    public CompanyGuaranteeEvent(double probGuarantee)  {
        this.probGuarantee = probGuarantee;
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
        targetsToGuaranteeRandom = new Random(DatagenParams.defaultSeed);
    }


    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
        targetsToGuaranteeRandom.setSeed(seed);
    }

    public List<Company> companyGuarantee(List<Company> companies, int blockId) {
        resetState(blockId);

        companies.forEach(company -> {
            if (randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_WHETHER_GURANTEE).nextDouble() < probGuarantee) {
                int targetsToGuarantee = targetsToGuaranteeRandom.nextInt(DatagenParams.maxTargetsToGuarantee);
                for (int j = 0; j < targetsToGuarantee; j++) {
                    Company toCompany = companies.get(randIndex.nextInt(companies.size())); // Choose a random company
                    if (company.canGuarantee(toCompany)) {
                        CompanyGuaranteeCompany.createCompanyGuaranteeCompany(
                            randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_GUARANTEE_DATE), company, toCompany);
                    }
                }
            }
        });

        return companies;
    }
}
