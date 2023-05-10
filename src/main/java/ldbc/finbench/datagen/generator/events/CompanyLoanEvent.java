package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyApplyLoan;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generator.generators.LoanGenerator;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyLoanEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;

    public CompanyLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
    }

    public List<CompanyApplyLoan> companyLoan(List<Company> companies, int blockId, GeneratorConfiguration conf) {
        resetState(blockId);
        List<CompanyApplyLoan> companyApplyLoans = new ArrayList<>();

        for (Company c : companies) {
            LoanGenerator loanGenerator = new LoanGenerator(conf);
            CompanyApplyLoan companyApplyLoan = CompanyApplyLoan.createCompanyApplyLoan(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                c,
                loanGenerator.generateLoan());
            companyApplyLoans.add(companyApplyLoan);
        }
        return companyApplyLoans;
    }
}
