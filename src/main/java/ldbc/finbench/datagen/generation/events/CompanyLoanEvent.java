package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyApplyLoan;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.generation.generators.LoanGenerator;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyLoanEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random random; // first random long is for personApply, second for companyApply

    public CompanyLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    private void resetState(LoanGenerator loanGenerator, int seed) {
        randomFarm.resetRandomGenerators(seed);
        random.setSeed(7654321L + 1234567 * seed);
        random.nextLong(); // Skip first random number for personApply
        loanGenerator.resetState(random.nextLong());
    }

    public List<CompanyApplyLoan> companyLoan(List<Company> companies, LoanGenerator loanGenerator, int blockId) {
        resetState(loanGenerator, blockId);
        List<CompanyApplyLoan> companyApplyLoans = new ArrayList<>();

        for (Company company : companies) {
            long applyDate = Dictionaries.dates.randomCompanyToLoanDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                                                                       company);
            CompanyApplyLoan companyApplyLoan = CompanyApplyLoan.createCompanyApplyLoan(
                applyDate, company, loanGenerator.generateLoan(applyDate));
            companyApplyLoans.add(companyApplyLoan);
        }
        return companyApplyLoans;
    }
}
