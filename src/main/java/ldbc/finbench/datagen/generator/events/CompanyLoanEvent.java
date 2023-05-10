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
    private RandomGeneratorFarm randomFarm;
    private Random random;

    public CompanyLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    public List<CompanyApplyLoan> companyLoan(List<Company> companies, int blockId, GeneratorConfiguration conf) {
        random.setSeed(blockId);
        List<CompanyApplyLoan> companyApplyLoans = new ArrayList<>();

        for (int i = 0; i < companies.size(); i++) {
            Company c = companies.get(i);
            LoanGenerator loanGenerator = new LoanGenerator(conf);

            if (loan()) {
                CompanyApplyLoan companyApplyLoan = CompanyApplyLoan.createCompanyApplyLoan(
                    randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                    c,
                    loanGenerator.generateLoan());
                companyApplyLoans.add(companyApplyLoan);
            }
        }
        return companyApplyLoans;
    }

    private boolean loan() {
        //TODO determine whether to generate loan
        return true;
    }
}
