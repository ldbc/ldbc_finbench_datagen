package ldbc.finbench.datagen.generator.events;

import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generator.generators.AccountGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyRegisterEvent {
    private RandomGeneratorFarm randomFarm;
    private Random random;

    public CompanyRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    public void companyRegister(List<Company> companies, int blockId) {
        random.setSeed(blockId);

        for (int i = 0; i < companies.size(); i++) {
            Company c = companies.get(i);
            AccountGenerator accountGenerator = new AccountGenerator();

            if (own()) {
                CompanyOwnAccount.createCompanyOwnAccount(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        c,
                        accountGenerator.generateAccount());
            }
        }
    }

    private boolean own() {
        //TODO determine whether to generate own
        return true;
    }
}
