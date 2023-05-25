package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
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
    private final Random random; // first random long is for personApply, second for companyApply
    private final Random numLoanRandom;

    public CompanyLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random(DatagenParams.defaultSeed);
        numLoanRandom = new Random(DatagenParams.defaultSeed);
    }

    private void resetState(LoanGenerator loanGenerator, int seed) {
        randomFarm.resetRandomGenerators(seed);
        random.setSeed(7654321L + 1234567L * seed);
        random.nextLong(); // Skip first random number for personApply
        long newSeed = random.nextLong();
        loanGenerator.resetState(newSeed);
        numLoanRandom.setSeed(newSeed);
    }

    public List<CompanyApplyLoan> companyLoan(List<Company> companies, LoanGenerator loanGenerator, int blockId) {
        resetState(loanGenerator, blockId);
        List<CompanyApplyLoan> companyApplyLoans = new ArrayList<>();

        for (Company company : companies) {
            for (int i = 0; i < numLoanRandom.nextInt(DatagenParams.maxLoans); i++) {
                long applyDate =
                    Dictionaries.dates.randomCompanyToLoanDate(randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_APPLY_LOAN_DATE),
                                                               company);
                Loan loan = loanGenerator.generateLoan(applyDate, "company");
                CompanyApplyLoan companyApplyLoan = CompanyApplyLoan.createCompanyApplyLoan(applyDate, company, loan);
                companyApplyLoans.add(companyApplyLoan);
            }
        }
        return companyApplyLoans;
    }
}
