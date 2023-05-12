package ldbc.finbench.datagen.generator.generators;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;
import ldbc.finbench.datagen.generator.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class LoanGenerator implements Serializable {
    private final double loanAmountMin;
    private final double loanAmountMax;
    private final DegreeDistribution degreeDistribution;
    private final RandomGeneratorFarm randomFarm;
    private int nextId = 0;

    public LoanGenerator(GeneratorConfiguration conf) {
        this.loanAmountMin = 0; // TODO: set by config
        this.loanAmountMax = 1000000; // TODO: set by config
        this.randomFarm = new RandomGeneratorFarm();
        this.degreeDistribution = DatagenParams.getDegreeDistribution();
        this.degreeDistribution.initialize();
    }

    private long composeLoanId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 44);
        long bucket = (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
            .getSimulationEnd());
        return (bucket << 44) | ((id & idMask));
    }

    // TODO: Reset not used yet
    private void resetState(int seed) {
        degreeDistribution.reset(seed);
        randomFarm.resetRandomGenerators(seed);
    }

    public Loan generateLoan() {
        long creationDate = Dictionaries.dates.randomLoanCreationDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
        long loanId = composeLoanId(nextId++, creationDate);
        double loanAmount =
            randomFarm.get(RandomGeneratorFarm.Aspect.LOAN_AMOUNT).nextDouble() * (loanAmountMax - loanAmountMin)
                + loanAmountMin;
        long maxDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.maxNumDegree);
        // Balance equals to the quota in a new loan
        return new Loan(loanId, loanAmount, loanAmount, creationDate, maxDegree);
    }

}
