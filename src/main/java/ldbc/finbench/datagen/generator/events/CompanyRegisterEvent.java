package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.CompanyOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.AccountOwnerEnum;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generator.generators.AccountGenerator;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyRegisterEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;

    public CompanyRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
    }

    public List<CompanyOwnAccount> companyRegister(List<Company> companies, AccountGenerator accountGenerator,
                                                   int blockId, GeneratorConfiguration conf) {
        resetState(blockId);
        List<CompanyOwnAccount> companyOwnAccounts = new ArrayList<>();

        for (Company c : companies) {
            Account account = accountGenerator.generateAccount();
            account.setAccountOwnerEnum(AccountOwnerEnum.COMPANY);
            account.setCompanyOwner(c);
            CompanyOwnAccount companyOwnAccount = CompanyOwnAccount.createCompanyOwnAccount(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE), c, account);
            companyOwnAccounts.add(companyOwnAccount);
        }
        return companyOwnAccounts;
    }

}
