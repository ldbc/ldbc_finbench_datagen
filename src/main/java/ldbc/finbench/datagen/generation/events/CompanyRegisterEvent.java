package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
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
        Random numAccRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_ACCOUNTS_PER_COMPANY);
        companies.forEach(company -> {
            int numAccounts = numAccRand.nextInt(DatagenParams.maxAccountsPerOwner);
            for (int i = 0; i < Math.max(1, numAccounts); i++) {
                Account account = accountGenerator.generateAccount(company.getCreationDate(), "company", blockId);
                CompanyOwnAccount.createCompanyOwnAccount(company, account, account.getCreationDate());
            }
        });

        return companies;
    }
}
