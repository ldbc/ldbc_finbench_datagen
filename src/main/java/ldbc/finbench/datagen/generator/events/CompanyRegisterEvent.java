package ldbc.finbench.datagen.generator.events;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generator.generators.AccountGenerator;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyRegisterEvent {
    private RandomGeneratorFarm randomFarm;
    private Random random;

    public CompanyRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    public List<CompanyOwnAccount> companyRegister(List<Company> companies, int blockId, GeneratorConfiguration conf) {
        random.setSeed(blockId);
        List<CompanyOwnAccount> companyOwnAccounts = new ArrayList<>();

        for (int i = 0; i < companies.size(); i++) {
            Company c = companies.get(i);
            AccountGenerator accountGenerator = new AccountGenerator(conf);

            if (own()) {
                CompanyOwnAccount companyOwnAccount = CompanyOwnAccount.createCompanyOwnAccount(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        c,
                        accountGenerator.generateAccount());
                companyOwnAccounts.add(companyOwnAccount);
            }
        }
        return companyOwnAccounts;
    }

    private boolean own() {
        //TODO determine whether to generate own
        return true;
    }
}
