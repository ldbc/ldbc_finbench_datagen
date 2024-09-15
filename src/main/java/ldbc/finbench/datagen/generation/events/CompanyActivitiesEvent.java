package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyApplyLoan;
import ldbc.finbench.datagen.entities.edges.CompanyGuaranteeCompany;
import ldbc.finbench.datagen.entities.edges.CompanyOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.generation.generators.AccountGenerator;
import ldbc.finbench.datagen.generation.generators.LoanGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyActivitiesEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public CompanyActivitiesEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public List<Company> companyActivities(List<Company> companies, AccountGenerator accountGenerator,
                                         LoanGenerator loanGenerator, int blockId) {
        resetState(blockId);
        accountGenerator.resetState(blockId);

        Random numAccRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_ACCOUNTS_PER_COMPANY);

        Random pickCompanyGuaRand = randomFarm.get(RandomGeneratorFarm.Aspect.PICK_COMPANY_GUARANTEE);
        Random numGuaranteesRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_GUARANTEES_PER_COMPANY);

        Random pickCompanyLoanRand = randomFarm.get(RandomGeneratorFarm.Aspect.PICK_COMPANY_FOR_LOAN);
        Random numLoansRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LOANS_PER_COMPANY);
        Random dateRand = randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_APPLY_LOAN_DATE);

        for (Company from : companies) {
            // register accounts
            int numAccounts = numAccRand.nextInt(DatagenParams.maxAccountsPerOwner);
            for (int i = 0; i < Math.max(1, numAccounts); i++) {
                Account account = accountGenerator.generateAccount(from.getCreationDate(), "company", blockId);
                CompanyOwnAccount.createCompanyOwnAccount(randomFarm, from, account, account.getCreationDate());
            }
            // guarantee other companies
            if (pickCompanyGuaRand.nextDouble() < DatagenParams.companyGuaranteeFraction) {
                int numGuarantees = numGuaranteesRand.nextInt(DatagenParams.maxTargetsToGuarantee);
                for (int i = 0; i < Math.max(1, numGuarantees); i++) {
                    Company to = companies.get(randIndex.nextInt(companies.size()));
                    if (from.canGuarantee(to)) {
                        CompanyGuaranteeCompany.createCompanyGuaranteeCompany(randomFarm, from, to);
                    }
                }
            }
            // apply loans
            if (pickCompanyLoanRand.nextDouble() < DatagenParams.companyLoanFraction) {
                int numLoans = numLoansRand.nextInt(DatagenParams.maxLoans);
                for (int i = 0; i < Math.max(1, numLoans); i++) {
                    long applyDate = Dictionaries.dates.randomCompanyToLoanDate(dateRand, from);
                    Loan to = loanGenerator.generateLoan(applyDate, "company", blockId);
                    CompanyApplyLoan.createCompanyApplyLoan(randomFarm, applyDate, from, to);
                }
            }
        }

        return companies;
    }
}
