package ldbc.finbench.datagen.generation.generators;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.generation.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class LoanGenerator implements Serializable {
    private final double loanAmountMin;
    private final double loanAmountMax;
    private final DegreeDistribution degreeDistribution;
    private final RandomGeneratorFarm randomFarm;
    private int nextId = 0;

    public LoanGenerator() {
        this.loanAmountMin = DatagenParams.minLoanAmount;
        this.loanAmountMax = DatagenParams.maxLoanAmount;
        this.randomFarm = new RandomGeneratorFarm();
        this.degreeDistribution = DatagenParams.getInDegreeDistribution();
        this.degreeDistribution.initialize();
    }

    private long composeLoanId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 44);
        long bucket = (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
            .getSimulationEnd());
        return (bucket << 44) | ((id & idMask));
    }

    public void resetState(long seed) {
        degreeDistribution.reset(seed);
        randomFarm.resetRandomGenerators(seed);
    }

    // Loan createDate is set when applying for a loan
    public Loan generateLoan(long creationDate) {
        long loanId = composeLoanId(nextId++, creationDate);
        double loanAmount =
            randomFarm.get(RandomGeneratorFarm.Aspect.LOAN_AMOUNT).nextDouble() * (loanAmountMax - loanAmountMin)
                + loanAmountMin;
        long maxDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.maxNumDegree);
        // Balance equals to the quota in a new loan
        return new Loan(loanId, loanAmount, loanAmount, creationDate, maxDegree);
    }

}
