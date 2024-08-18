package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyApplyLoan;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.generation.generators.LoanGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyLoanEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;

    public CompanyLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
    }

    public List<Company> companyLoan(List<Company> companies, LoanGenerator loanGenerator, int blockId) {
        resetState(blockId);
        loanGenerator.resetState(blockId);

        Random pickCompanyRand = randomFarm.get(RandomGeneratorFarm.Aspect.PICK_COMPANY_FOR_LOAN);
        Random numLoansRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LOANS_PER_COMPANY);
        Random dateRand = randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_APPLY_LOAN_DATE);

        int numCompaniesToTake = (int) (companies.size() * DatagenParams.companyLoanFraction);
        for (int i = 0; i < numCompaniesToTake; i++) {
            Company from = companies.get(pickCompanyRand.nextInt(companies.size()));
            int numLoans = numLoansRand.nextInt(DatagenParams.maxLoans);
            for (int j = 0; j < Math.max(1, numLoans); j++) {
                long applyDate = Dictionaries.dates.randomCompanyToLoanDate(dateRand, from);
                Loan to = loanGenerator.generateLoan(applyDate, "company", blockId);
                CompanyApplyLoan.createCompanyApplyLoan(randomFarm, applyDate, from, to);
            }
        }

        return companies;
    }
}
