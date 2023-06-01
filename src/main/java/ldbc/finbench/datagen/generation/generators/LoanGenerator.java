package ldbc.finbench.datagen.generation.generators;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.generation.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class LoanGenerator implements Serializable {
    private final long loanAmountMin;
    private final long loanAmountMax;
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

    private long composeLoanId(long id, long date, String personOrCompany, int blockId) {
        // the bits are composed as follows from left to right:
        // 1 bit for sign, 1 bit for type, 14 bits for bucket ranging from 0 to 365 * numYears, 10 bits for blockId,
        // 38 bits for id
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 38);
        // each bucket is 1 day, range from 0 to 365 * numYears
        long bucket = (long) (365 * DatagenParams.numYears * (date - Dictionaries.dates.getSimulationStart())
            / (double) (Dictionaries.dates.getSimulationEnd() - Dictionaries.dates.getSimulationStart()));
        if (personOrCompany.equals("company")) {
            return (bucket << 48) | (long) blockId << 38 | ((id & idMask));
        } else {
            return 0x1L << 62 | (bucket << 48) | (long) blockId << 38 | ((id & idMask));
        }
    }

    public void resetState(long seed) {
        degreeDistribution.reset(seed);
        randomFarm.resetRandomGenerators(seed);
    }

    // Loan createDate is set when applying for a loan
    public Loan generateLoan(long creationDate, String type, int blockId) {
        long loanId = composeLoanId(nextId++, creationDate, type, blockId);
        double loanAmount =
            randomFarm.get(RandomGeneratorFarm.Aspect.LOAN_AMOUNT).nextInt((int) (loanAmountMax - loanAmountMin))
                + loanAmountMin;
        long maxDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.tsfMaxNumDegree);
        String usage =
            Dictionaries.loanUsages.getUniformDistRandomText(randomFarm.get(RandomGeneratorFarm.Aspect.LOAN_USAGE));
        double interestRate =
            randomFarm.get(RandomGeneratorFarm.Aspect.LOAN_INTEREST_RATE).nextDouble() * DatagenParams.maxLoanInterest;
        // Balance equals to the quota in a new loan
        return new Loan(loanId, loanAmount, loanAmount, creationDate, maxDegree, usage, interestRate);
    }

}
