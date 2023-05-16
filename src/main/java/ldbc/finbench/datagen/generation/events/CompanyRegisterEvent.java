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
    private final Random random; // first random long is for personRegister, second for companyRegister
    private final Random numAccountsRandom;

    public CompanyRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random(DatagenParams.defaultSeed);
        numAccountsRandom = new Random(DatagenParams.defaultSeed);
    }

    private void resetState(AccountGenerator accountGenerator, int seed) {
        randomFarm.resetRandomGenerators(seed);
        random.setSeed(7654321L + 1234567 * seed);
        random.nextLong(); // Skip first random number for personRegister
        long newSeed = random.nextLong();
        accountGenerator.resetState(newSeed);
        numAccountsRandom.setSeed(newSeed);
    }

    public List<CompanyOwnAccount> companyRegister(List<Company> companies, AccountGenerator accountGenerator,
                                                   int blockId) {
        resetState(accountGenerator, blockId);
        List<CompanyOwnAccount> companyOwnAccounts = new ArrayList<>();

        for (Company company : companies) {
            for (int i = 0; i < Math.max(1, numAccountsRandom.nextInt(DatagenParams.maxAccountsPerOwner)); i++) {
                Account account = accountGenerator.generateAccount();
                CompanyOwnAccount companyOwnAccount =
                    CompanyOwnAccount.createCompanyOwnAccount(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), company,
                                                              account);
                companyOwnAccounts.add(companyOwnAccount);
            }
        }
        return companyOwnAccounts;
    }

}
