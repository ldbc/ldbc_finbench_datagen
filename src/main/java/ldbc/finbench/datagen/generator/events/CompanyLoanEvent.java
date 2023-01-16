package ldbc.finbench.datagen.generator.events;

import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.CompanyApplyLoan;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generator.generators.LoanGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyLoanEvent {
    private RandomGeneratorFarm randomFarm;
    private Random random;

    public CompanyLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    public void companyLoan(List<Company> companies, int blockId) {
        random.setSeed(blockId);

        for (int i = 0; i < companies.size(); i++) {
            Company c = companies.get(i);
            LoanGenerator loanGenerator = new LoanGenerator();

            if (loan()) {
                CompanyApplyLoan.createCompanyApplyLoan(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        c,
                        loanGenerator.generateLoan());
            }

        }
    }

    private boolean loan() {
        //TODO determine whether to generate loan
        return true;
    }

    private boolean deposit() {
        //TODO determine whether to generate deposit
        return true;
    }

    private boolean transfer() {
        //TODO determine whether to generate transfer
        return true;
    }

    private boolean repay() {
        //TODO determine whether to generate repay
        return true;
    }
}
