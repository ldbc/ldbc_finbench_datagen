package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyGuaranteeCompany;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyGuaranteeEvent implements Serializable {
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public CompanyGuaranteeEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }

    public List<CompanyGuaranteeCompany> companyGuarantee(List<Company> companies, int blockId) {
        random.setSeed(blockId);
        List<CompanyGuaranteeCompany> companyGuaranteeCompanies = new ArrayList<>();

        for (int i = 0; i < companies.size(); i++) {
            Company c = companies.get(i);
            int companyIndex = randIndex.nextInt(companies.size());

            if (guarantee()) {
                CompanyGuaranteeCompany companyGuaranteeCompany = CompanyGuaranteeCompany.createCompanyGuaranteeCompany(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        c,
                        companies.get(companyIndex));
                companyGuaranteeCompanies.add(companyGuaranteeCompany);
            }
        }
        return companyGuaranteeCompanies;
    }

    private boolean guarantee() {
        //TODO determine whether to generate guarantee
        return true;
    }
}
