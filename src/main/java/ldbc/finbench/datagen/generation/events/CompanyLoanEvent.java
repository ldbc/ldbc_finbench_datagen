package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
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

    public List<CompanyApplyLoan> companyLoan(List<Company> companies, LoanGenerator loanGenerator, int blockId) {
        resetState(blockId);
        loanGenerator.resetState(blockId);
        List<CompanyApplyLoan> companyApplyLoans = new ArrayList<>();

        for (Company company : companies) {
            int numLoans =
                randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LOANS_PER_COMPANY).nextInt(DatagenParams.maxLoans);
            for (int i = 0; i < Math.max(1, numLoans); i++) {
                long applyDate =
                    Dictionaries.dates.randomCompanyToLoanDate(
                        randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_APPLY_LOAN_DATE),
                        company);
                Loan loan = loanGenerator.generateLoan(applyDate, "company", blockId);
                CompanyApplyLoan companyApplyLoan = CompanyApplyLoan.createCompanyApplyLoan(applyDate, company, loan);
                companyApplyLoans.add(companyApplyLoan);
            }
        }
        return companyApplyLoans;
    }
}
