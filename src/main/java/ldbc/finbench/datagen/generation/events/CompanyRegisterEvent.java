package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.CompanyOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.generators.AccountGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyRegisterEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;

    public CompanyRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
    }

    public List<Company> companyRegister(List<Company> companies, AccountGenerator accountGenerator, int blockId) {
        resetState(blockId);
        accountGenerator.resetState(blockId);
        companies.forEach(company -> {
            int numAccounts = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_ACCOUNTS_PER_COMPANY)
                                        .nextInt(DatagenParams.maxAccountsPerOwner);
            // Each company has at least one account
            for (int i = 0; i < Math.max(1, numAccounts); i++) {
                Account account = accountGenerator.generateAccount(company.getCreationDate(), "company", blockId);
                CompanyOwnAccount.createCompanyOwnAccount(company, account, account.getCreationDate());
            }
        });

        return companies;
    }
}
