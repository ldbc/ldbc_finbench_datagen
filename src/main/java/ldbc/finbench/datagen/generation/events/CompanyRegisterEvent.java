package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
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
    private final Random numAccountsRandom;

    public CompanyRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
        numAccountsRandom = new Random(DatagenParams.defaultSeed);
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        numAccountsRandom.setSeed(seed);
    }

    public List<CompanyOwnAccount> companyRegister(List<Company> companies, AccountGenerator accountGenerator,
                                                   int blockId) {
        resetState(blockId);
        accountGenerator.resetState(blockId);
        List<CompanyOwnAccount> companyOwnAccounts = new ArrayList<>();
        for (Company company : companies) {
            // Each company has at least one account
            for (int i = 0; i < Math.max(1, numAccountsRandom.nextInt(DatagenParams.maxAccountsPerOwner)); i++) {
                // Account created after company creation date
                Account account = accountGenerator.generateAccount(company.getCreationDate(), "company", blockId);
                CompanyOwnAccount companyOwnAccount =
                    CompanyOwnAccount.createCompanyOwnAccount(randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_OWN_ACCOUNT_DATE), company,
                                                              account);
                companyOwnAccounts.add(companyOwnAccount);
            }
        }
        return companyOwnAccounts;
    }

}
