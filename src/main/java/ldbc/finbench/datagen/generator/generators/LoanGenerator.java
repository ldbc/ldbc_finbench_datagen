package ldbc.finbench.datagen.generator.generators;

import java.util.Random;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;
import ldbc.finbench.datagen.generator.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class LoanGenerator {

    private DegreeDistribution degreeDistribution;
    private RandomGeneratorFarm randomFarm;
    private int nextId  = 0;

    private long composeLoanId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 44);
        long bucket = (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
                .getSimulationEnd());
        return (bucket << 44) | ((id & idMask));
    }

    public Loan generateLoan() {

        long creationDate = Dictionaries.dates.randomLoanCreationDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
        long loanId = composeLoanId(nextId++, creationDate);
        long loanAmount = new Random().nextLong();
        long loanBalance = new Random().nextLong();
        long maxDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.maxNumDegree);

        return new Loan(loanId,creationDate,loanAmount,loanBalance,maxDegree);
    }

}
