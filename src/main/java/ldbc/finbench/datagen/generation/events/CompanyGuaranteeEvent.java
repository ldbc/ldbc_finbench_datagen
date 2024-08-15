package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
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

    public List<Company> companyGuarantee(List<Company> companies, int blockId) {
        resetState(blockId);

        Collections.shuffle(companies, randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_GURANTEE_SHUFFLE));
        int numCompaniesToTake = (int) (companies.size() * DatagenParams.companyGuaranteeFraction);
        for (int i = 0; i < numCompaniesToTake; i++) {
            Company from = companies.get(i);
            int targetsToGuarantee = targetsToGuaranteeRandom.nextInt(DatagenParams.maxTargetsToGuarantee);
            for (int j = 0; j < targetsToGuarantee; j++) {
                Company to = companies.get(randIndex.nextInt(companies.size())); // Choose a random company
                if (from.canGuarantee(to)) {
                    CompanyGuaranteeCompany.createCompanyGuaranteeCompany(
                        randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_GUARANTEE_DATE), from, to);
                }
            }
        }

        return companies;
    }
}
