package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.AccountOwnerEnum;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.generators.AccountGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyRegisterEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random random; // first random long is for personRegister, second for companyRegister

    public CompanyRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    private void resetState(AccountGenerator accountGenerator, int seed) {
        randomFarm.resetRandomGenerators(seed);
        random.setSeed(7654321L + 1234567 * seed);
        random.nextLong(); // Skip first random number for personRegister
        accountGenerator.resetState(random.nextLong());
    }

    public List<CompanyOwnAccount> companyRegister(List<Company> companies, AccountGenerator accountGenerator,
                                                   int blockId) {
        resetState(accountGenerator, blockId);
        List<CompanyOwnAccount> companyOwnAccounts = new ArrayList<>();

        for (Company c : companies) {
            Account account = accountGenerator.generateAccount();
            account.setAccountOwnerEnum(AccountOwnerEnum.COMPANY);
            account.setCompanyOwner(c);
            CompanyOwnAccount companyOwnAccount =
                CompanyOwnAccount.createCompanyOwnAccount(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), c, account);
            companyOwnAccounts.add(companyOwnAccount);
        }
        return companyOwnAccounts;
    }

}
