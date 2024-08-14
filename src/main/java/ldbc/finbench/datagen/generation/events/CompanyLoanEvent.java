package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
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

        Collections.shuffle(companies, randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_LOAN_SHUFFLE));
        int numCompaniesToTake = (int) (companies.size() * DatagenParams.companyLoanFraction);
        for (int i = 0; i < numCompaniesToTake; i++) {
            Company from = companies.get(i);
            int numLoans =
                randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LOANS_PER_COMPANY).nextInt(DatagenParams.maxLoans);
            for (int j = 0; j < Math.max(1, numLoans); j++) {
                long applyDate =
                    Dictionaries.dates.randomCompanyToLoanDate(
                        randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_APPLY_LOAN_DATE),
                        from);
                Loan to = loanGenerator.generateLoan(applyDate, "company", blockId);
                String organization = Dictionaries.loanOrganizations.getUniformDistRandomText(
                    randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_APPLY_LOAN_ORGANIZATION));
                CompanyApplyLoan.createCompanyApplyLoan(applyDate, from, to, organization);
            }
        }

        return companies;
    }
}
